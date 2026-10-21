import base64
import json
import logging
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from typing import Dict, Any, List, Optional
import functions_framework
from flask import Request, jsonify

# Configure standard logging for Cloud Run
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("collibra-sq-gatekeeper")

# Default configurations
DEFAULT_PROJECT = os.environ.get("GCP_PROJECT", "crp-sdx-cx-ia")
DEFAULT_LOCATION = os.environ.get("BQ_LOCATION", "us-central1")
# Si AUTO_WAIT_AND_DISABLE es true, monitorea la corrida recién disparada y apaga el SQ apenas termine
AUTO_WAIT_AND_DISABLE = os.environ.get("AUTO_WAIT_AND_DISABLE", "true").lower() == "true"
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "5"))
MAX_WAIT_SECONDS = int(os.environ.get("MAX_WAIT_SECONDS", "300"))

# Mapping of Evaluated Tables/Datasets to Dependent Scheduled Queries (Transfer Configs)
TABLE_TO_QUERIES_MAP = {
    "poc_raw.pedidos_raw": [
        # Raw to Silver
        "projects/911068210905/locations/us-central1/transferConfigs/6ad25586-0000-263d-b9ea-883d24f7a90c",
        # Silver to Gold
        "projects/911068210905/locations/us-central1/transferConfigs/6acdc8c7-0000-2248-9678-2405887bf2d0"
    ]
}


def get_default_target_queries() -> List[str]:
    """Retrieve fallback target queries from env or default mapping."""
    env_queries = os.environ.get("TARGET_TRANSFER_CONFIGS", "")
    if env_queries:
        return [q.strip() for q in env_queries.split(",") if q.strip()]
    return TABLE_TO_QUERIES_MAP.get("poc_raw.pedidos_raw", [])


def find_gcloud_binary() -> str:
    """Locate the gcloud executable on Windows or Linux."""
    gcloud_path = shutil.which("gcloud") or shutil.which("gcloud.cmd")
    if gcloud_path:
        return gcloud_path

    common_paths = [
        os.path.expandvars(r"%LOCALAPPDATA%\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"),
        r"C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
        r"C:\Program Files\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
    ]
    for path in common_paths:
        if os.path.exists(path):
            return path
    return "gcloud"


def get_gcloud_access_token() -> str:
    """Obtain an access token from local gcloud CLI."""
    gcloud_bin = find_gcloud_binary()
    token_proc = subprocess.run(
        [gcloud_bin, "auth", "print-access-token"],
        capture_output=True,
        text=True,
        check=True,
        shell=False
    )
    token = token_proc.stdout.strip()
    if not token:
        raise RuntimeError("Could not obtain access token from gcloud CLI.")
    return token


def wait_and_disable_via_rest_api(config_name: str, run_name: str, token: str) -> Dict[str, Any]:
    """
    Monitorea la ejecución del SQ vía REST API y, en cuanto termina
    (SUCCEEDED, FAILED o CANCELLED), vuelve a deshabilitar el SQ (disabled=True).
    """
    run_url = f"https://bigquerydatatransfer.googleapis.com/v1/{run_name}"
    headers = {"Authorization": f"Bearer {token}"}
    terminal_states = {"SUCCEEDED", "FAILED", "CANCELLED"}

    elapsed = 0
    final_state = "UNKNOWN"
    logger.info(f"Monitoreando ejecución {run_name} para deshabilitar el SQ al terminar...")

    while elapsed < MAX_WAIT_SECONDS:
        req = urllib.request.Request(run_url, headers=headers, method="GET")
        with urllib.request.urlopen(req) as resp:
            run_data = json.loads(resp.read().decode("utf-8"))
            final_state = run_data.get("state", "UNKNOWN")

        if final_state in terminal_states:
            logger.info(f"El SQ terminó con estado '{final_state}'. Procediendo a deshabilitar {config_name}...")
            break

        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

    # Siempre deshabilitar el SQ al terminar (o al agotar el tiempo de espera)
    patch_url = (
        f"https://bigquerydatatransfer.googleapis.com/v1/{config_name}"
        f"?updateMask=disabled,scheduleOptions.disableAutoScheduling"
    )
    patch_payload = json.dumps({
        "disabled": True,
        "scheduleOptions": {"disableAutoScheduling": True}
    }).encode("utf-8")

    patch_req = urllib.request.Request(
        patch_url,
        data=patch_payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        method="PATCH"
    )
    with urllib.request.urlopen(patch_req) as patch_resp:
        patch_result = json.loads(patch_resp.read().decode("utf-8"))

    return {
        "final_run_state": final_state,
        "disabled_after_completion": patch_result.get("disabled", True)
    }


def update_via_dts_rest_api(
    config_name: str,
    disable: bool,
    execute_now: bool = False,
    disable_after_run: bool = True
) -> Dict[str, Any]:
    """
    Fallback para desarrollo local usando el token de gcloud CLI.
    1. Quita el horario automático (disableAutoScheduling = True).
    2. Actualiza 'disabled' (False para habilitar, True para deshabilitar).
    3. Si execute_now=True, dispara la ejecución inmediata con :startManualRuns.
    4. Si disable_after_run=True, espera a que termine y vuelve a dejar disabled=True.
    """
    token = get_gcloud_access_token()

    # Paso 1 y 2: Quitar horario por tiempo y actualizar estado enabled/disabled
    patch_url = (
        f"https://bigquerydatatransfer.googleapis.com/v1/{config_name}"
        f"?updateMask=disabled,scheduleOptions.disableAutoScheduling"
    )
    patch_payload = json.dumps({
        "disabled": disable,
        "scheduleOptions": {
            "disableAutoScheduling": True
        }
    }).encode("utf-8")

    req = urllib.request.Request(
        patch_url,
        data=patch_payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        method="PATCH"
    )

    try:
        with urllib.request.urlopen(req) as resp:
            patch_response = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"DTS REST API PATCH error ({e.code}): {err_body}") from e

    # Paso 3: Si está habilitado y se requiere ejecución inmediata On-Demand
    triggered_runs = []
    post_run_disable_info = None
    if not disable and execute_now:
        run_url = f"https://bigquerydatatransfer.googleapis.com/v1/{config_name}:startManualRuns"
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        run_payload = json.dumps({
            "requestedRunTime": now_iso
        }).encode("utf-8")

        run_req = urllib.request.Request(
            run_url,
            data=run_payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            },
            method="POST"
        )
        try:
            with urllib.request.urlopen(run_req) as run_resp:
                run_data = json.loads(run_resp.read().decode("utf-8"))
                runs = run_data.get("runs", [])
                triggered_runs = [r.get("name") for r in runs]
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"DTS REST API startManualRuns error ({e.code}): {err_body}") from e

        # Paso 4: Siempre que termine la ejecución, volver a deshabilitar el SQ
        if disable_after_run and triggered_runs:
            post_run_disable_info = wait_and_disable_via_rest_api(
                config_name=config_name,
                run_name=triggered_runs[0],
                token=token
            )

    return {
        "config": patch_response,
        "triggered_runs": triggered_runs,
        "post_run_disable": post_run_disable_info
    }


def wait_and_disable_sdk(client: Any, config_name: str, run_name: str) -> Dict[str, Any]:
    """
    Monitorea la corrida del SQ con el SDK oficial de BigQuery DTS y,
    en cuanto termina, deshabilita el SQ (disabled=True).
    """
    from google.cloud import bigquery_datatransfer_v1
    from google.protobuf import field_mask_pb2

    # Estados terminales en TransferState: SUCCEEDED (4), FAILED (5), CANCELLED (6)
    terminal_states = {
        bigquery_datatransfer_v1.TransferState.SUCCEEDED,
        bigquery_datatransfer_v1.TransferState.FAILED,
        bigquery_datatransfer_v1.TransferState.CANCELLED,
    }

    elapsed = 0
    final_state_name = "UNKNOWN"
    logger.info(f"Monitoreando ejecución {run_name} para deshabilitar el SQ al terminar...")

    while elapsed < MAX_WAIT_SECONDS:
        run_obj = client.get_transfer_run(name=run_name)
        final_state_name = bigquery_datatransfer_v1.TransferState(run_obj.state).name
        if run_obj.state in terminal_states:
            logger.info(f"Ejecución {run_name} finalizó con estado {final_state_name}. Deshabilitando SQ...")
            break
        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

    # Deshabilitar el SQ inmediatamente después de terminar
    disable_config = bigquery_datatransfer_v1.TransferConfig(
        name=config_name,
        disabled=True,
        schedule_options=bigquery_datatransfer_v1.ScheduleOptions(
            disable_auto_scheduling=True
        )
    )
    update_mask = field_mask_pb2.FieldMask(
        paths=["disabled", "schedule_options.disable_auto_scheduling"]
    )
    updated = client.update_transfer_config(
        request={
            "transfer_config": disable_config,
            "update_mask": update_mask
        }
    )
    return {
        "final_run_state": final_state_name,
        "disabled_after_completion": updated.disabled
    }


def set_scheduled_queries_status(
    config_names: List[str],
    disable: bool,
    execute_now: bool = False,
    disable_after_run: bool = True
) -> Dict[str, Any]:
    """
    Enables or Disables a list of BigQuery Scheduled Queries:
    1. Always removes time-based scheduling (disable_auto_scheduling = True -> On-Demand).
    2. Sets disabled = False (enable) or disabled = True (disable).
    3. If execute_now = True and disabled = False, triggers an immediate manual run.
    4. If disable_after_run = True, automatically disables the SQ (disabled = True) once the run finishes.
    """
    from google.cloud import bigquery_datatransfer_v1
    from google.protobuf import field_mask_pb2
    from google.protobuf import timestamp_pb2
    import google.auth.exceptions

    results = {}
    action_str = "Disabling" if disable else "Enabling (On-Demand) + Executing"

    try:
        client = bigquery_datatransfer_v1.DataTransferServiceClient()
    except google.auth.exceptions.DefaultCredentialsError:
        client = None

    for name in config_names:
        try:
            logger.info(f"{action_str} Scheduled Query: {name}")

            if client is not None:
                try:
                    # 1. Quitar programación por tiempo (On-Demand) y actualizar estado disabled
                    transfer_config = bigquery_datatransfer_v1.TransferConfig(
                        name=name,
                        disabled=disable,
                        schedule_options=bigquery_datatransfer_v1.ScheduleOptions(
                            disable_auto_scheduling=True
                        )
                    )
                    update_mask = field_mask_pb2.FieldMask(
                        paths=["disabled", "schedule_options.disable_auto_scheduling"]
                    )

                    response = client.update_transfer_config(
                        request={
                            "transfer_config": transfer_config,
                            "update_mask": update_mask
                        }
                    )

                    # 2. Si se habilitó, ejecutar inmediatamente On-Demand
                    triggered_runs = []
                    post_run_disable_info = None
                    if not disable and execute_now:
                        now_ts = timestamp_pb2.Timestamp()
                        now_ts.GetCurrentTime()

                        run_response = client.start_manual_transfer_runs(
                            request={
                                "parent": name,
                                "requested_run_time": now_ts
                            }
                        )
                        triggered_runs = [r.name for r in run_response.runs]
                        logger.info(f"Triggered manual run for {name}: {triggered_runs}")

                        # 3. Siempre que termine, volver a deshabilitar el SQ
                        if disable_after_run and triggered_runs:
                            post_run_disable_info = wait_and_disable_sdk(
                                client=client,
                                config_name=name,
                                run_name=triggered_runs[0]
                            )

                    final_disabled_state = (
                        post_run_disable_info["disabled_after_completion"]
                        if post_run_disable_info
                        else response.disabled
                    )

                    results[name] = {
                        "status": "success",
                        "method": "adc_sdk",
                        "on_demand_only": response.schedule_options.disable_auto_scheduling,
                        "disabled_during_run": response.disabled,
                        "triggered_runs": triggered_runs,
                        "post_run_disable": post_run_disable_info,
                        "final_disabled_state": final_disabled_state
                    }
                    continue
                except google.auth.exceptions.DefaultCredentialsError:
                    logger.info("ADC not found, falling back to gcloud CLI token via REST API...")

            # Fallback a REST API usando el token de gcloud CLI
            rest_resp = update_via_dts_rest_api(
                config_name=name,
                disable=disable,
                execute_now=execute_now,
                disable_after_run=disable_after_run
            )
            cfg = rest_resp.get("config", {})
            post_run_info = rest_resp.get("post_run_disable")
            final_disabled = (
                post_run_info["disabled_after_completion"]
                if post_run_info
                else cfg.get("disabled", disable)
            )
            results[name] = {
                "status": "success",
                "method": "gcloud_rest_fallback",
                "on_demand_only": cfg.get("scheduleOptions", {}).get("disableAutoScheduling", True),
                "disabled_during_run": cfg.get("disabled", disable),
                "triggered_runs": rest_resp.get("triggered_runs", []),
                "post_run_disable": post_run_info,
                "final_disabled_state": final_disabled
            }
            logger.info(
                f"Successfully processed {name}. "
                f"Runs={rest_resp.get('triggered_runs')}, FinalDisabled={final_disabled}"
            )

        except Exception as e:
            logger.error(f"Error updating Scheduled Query {name}: {e}", exc_info=True)
            results[name] = {
                "status": "error",
                "error_message": str(e)
            }

    return results


def extract_transfer_config_from_run_name(run_or_config_name: str) -> Optional[str]:
    """
    Si BigQuery DTS envía por Pub/Sub el nombre de la corrida:
    'projects/911.../locations/us-central1/transferConfigs/6ad.../runs/65f...'
    extrae el nombre del TransferConfig padre:
    'projects/911.../locations/us-central1/transferConfigs/6ad...'
    """
    if "/runs/" in run_or_config_name:
        return run_or_config_name.split("/runs/")[0]
    if "/transferConfigs/" in run_or_config_name:
        return run_or_config_name
    return None


def process_collibra_sq_webhook(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evalúa el payload recibido (desde Collibra o desde el evento de fin de corrida de BigQuery DTS):
    - Si es un evento de fin de ejecución del SQ (Pub/Sub de BQ DTS o status FINISHED/COMPLETED):
      -> Deshabilita inmediatamente el SQ (disabled = True).
    - Si Collibra envía SUCCESS / PASSED / RESOLVED:
      -> Quita horario, habilita el SQ (disabled = False), lo ejecuta On-Demand y al terminar lo vuelve a deshabilitar (disabled = True).
    - Si Collibra envía FAILED / BROKEN / ERROR:
      -> Quita horario y mantiene el SQ deshabilitado (disabled = True).
    """
    logger.info(f"Processing Payload: {json.dumps(payload)}")

    # Caso A: Evento nativo de BigQuery DTS al terminar un Scheduled Query (vía Pub/Sub Trigger 2)
    bq_run_state = str(payload.get("state", "")).upper()
    bq_resource_name = str(payload.get("name", ""))
    if bq_run_state in ["SUCCEEDED", "FAILED", "CANCELLED"] and "/transferConfigs/" in bq_resource_name:
        config_name = extract_transfer_config_from_run_name(bq_resource_name)
        logger.info(
            f"Detectado fin de ejecución de SQ ({bq_run_state}) para {config_name}. "
            f"Deshabilitando SQ inmediatamente..."
        )
        sq_results = set_scheduled_queries_status(
            config_names=[config_name],
            disable=True,
            execute_now=False,
            disable_after_run=False
        )
        return {
            "event_type": "SQ_RUN_FINISHED",
            "sq_run_state": bq_run_state,
            "action_taken": "DISABLED_AFTER_COMPLETION",
            "queries_affected": sq_results
        }

    # Caso B: Webhook de Collibra o llamada directa con status
    status = (
        payload.get("status")
        or payload.get("dq_status")
        or payload.get("state")
        or payload.get("event_type")
    )

    if not status and "incident" in payload:
        inc_state = payload["incident"].get("state", "").upper()
        if inc_state in ["OPEN", "NEW", "ACTIVE"]:
            status = "FAILED"
        elif inc_state in ["RESOLVED", "CLOSED"]:
            status = "SUCCESS"

    if not status:
        status = "UNKNOWN"
    else:
        status = str(status).upper()

    dataset = payload.get("dataset", "")
    table = payload.get("table", payload.get("dataset_name", ""))
    full_table_name = f"{dataset}.{table}".strip(".")

    target_queries = payload.get("target_queries", [])
    if not target_queries and full_table_name in TABLE_TO_QUERIES_MAP:
        target_queries = TABLE_TO_QUERIES_MAP[full_table_name]
    if not target_queries:
        target_queries = get_default_target_queries()

    if not target_queries:
        logger.warning(f"No Scheduled Queries mapped for table '{full_table_name}'.")
        return {"status": "ignored", "reason": "No target queries configured"}

    # Caso B.1: Evento explícito de que el SQ terminó -> Deshabilitar SQ
    if status in ["FINISHED", "COMPLETED", "SQ_FINISHED", "DISABLE"]:
        logger.info("Solicitud de cierre post-ejecución recibida. Deshabilitando Scheduled Queries...")
        disable = True
        execute_now = False
        action_taken = "DISABLED_AFTER_COMPLETION"

    # Caso B.2: Calidad falló -> Circuito Abierto (Deshabilitar y NO ejecutar)
    elif status in ["FAILED", "ERROR", "BROKEN", "ALERT", "CRITICAL", "REJECTED"]:
        logger.warning(f"Data Quality FAILED ({status}) on {full_table_name}. Disabling Scheduled Queries!")
        disable = True
        execute_now = False
        action_taken = "CIRCUIT_OPENED_QUERIES_DISABLED"

    # Caso B.3: Calidad pasó -> Habilitar, Ejecutar On-Demand y Deshabilitar al Terminar
    elif status in ["SUCCESS", "PASSED", "OK", "RESOLVED", "APPROVED", "COMPLETED_DQ"]:
        logger.info(
            f"Data Quality PASSED ({status}) on {full_table_name}. "
            f"Enabling, Executing On-Demand, and Disabling upon completion!"
        )
        disable = False
        execute_now = True
        action_taken = "ENABLED_EXECUTED_AND_DISABLED_ON_FINISH"
    else:
        logger.info(f"Unrecognized status '{status}'. Taking no action.")
        return {"status": "ignored", "reason": f"Unrecognized status {status}"}

    sq_results = set_scheduled_queries_status(
        config_names=target_queries,
        disable=disable,
        execute_now=execute_now,
        disable_after_run=AUTO_WAIT_AND_DISABLE
    )

    return {
        "evaluated_table": full_table_name or "default",
        "dq_status_received": status,
        "action_taken": action_taken,
        "queries_affected": sq_results
    }


@functions_framework.http
def sq_webhook_http(request: Request):
    """HTTP Cloud Function / Cloud Run Entrypoint."""
    if request.method not in ["POST", "PUT"]:
        return jsonify({"error": "Method not allowed. Use POST."}), 405

    try:
        raw_data = request.get_json(silent=True)
        if not raw_data:
            return jsonify({"error": "Empty or invalid JSON payload"}), 400

        # Soporta mensajes Push de Pub/Sub (tanto de Collibra como de BigQuery DTS al terminar un SQ)
        if "message" in raw_data and "data" in raw_data["message"]:
            b64_data = raw_data["message"]["data"]
            decoded_str = base64.b64decode(b64_data).decode("utf-8")
            payload = json.loads(decoded_str)
        else:
            payload = raw_data

        result = process_collibra_sq_webhook(payload)
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error processing webhook: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500