import datetime as _dt
import json
import logging
import os
import urllib.error
import urllib.request


LOGGER = logging.getLogger(__name__)
DEFAULT_TIMEOUT_SECONDS = 5


def _base_url():
    value = os.getenv("PROCESS_MONITOR_BASE_URL", "").strip().rstrip("/")
    if not value:
        return None
    return value


def _token():
    return os.getenv("PROCESS_MONITOR_TOKEN", "").strip()


def _utc_now_text():
    return _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _post(path, payload, timeout=DEFAULT_TIMEOUT_SECONDS):
    base_url = _base_url()
    if not base_url:
        LOGGER.warning("PROCESS_MONITOR_BASE_URL is not configured; skip monitor report")
        return False

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        f"{base_url}{path}",
        data=body,
        headers={
            "Content-Type": "application/json",
            **({"Authorization": f"Bearer {_token()}"} if _token() else {}),
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if 200 <= response.status < 300:
                return True
            LOGGER.warning("monitor report failed with status=%s", response.status)
            return False
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        LOGGER.warning("monitor report failed: %s", exc)
        return False


def report_process_status(
    process_code,
    status,
    execution_id=None,
    start_time=None,
    end_time=None,
    duration=None,
    latest_data_time=None,
    total_records=None,
    error_message=None,
):
    payload = {
        "processCode": process_code,
        "status": status,
        "executionId": execution_id,
        "startTime": start_time,
        "endTime": end_time,
        "duration": duration,
        "latestDataTime": latest_data_time,
        "totalRecords": total_records,
        "errorMessage": error_message,
    }
    return _post("/api/process-monitor/update-status", payload)


def report_component_status(
    process_code,
    component_code,
    component_name,
    status,
    component_type="list",
    execution_id=None,
    start_time=None,
    end_time=None,
    duration=None,
    total_records=None,
    success_count=None,
    failed_count=None,
    error_message=None,
    last_heartbeat_at=None,
):
    payload = {
        "processCode": process_code,
        "componentCode": component_code,
        "componentName": component_name,
        "componentType": component_type,
        "status": status,
        "executionId": execution_id,
        "startTime": start_time,
        "EndTime": end_time,
        "duration": duration,
        "totalRecords": total_records,
        "successCount": success_count,
        "failedCount": failed_count,
        "errorMessage": error_message,
        "lastHeartbeatAt": last_heartbeat_at or _utc_now_text(),
    }
    if end_time is not None:
        payload["endTime"] = end_time
    payload.pop("EndTime", None)
    return _post("/api/process-monitor/update-component-status", payload)


def safe_report(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        LOGGER.warning("monitor report raised unexpectedly: %s", exc)
        return False
