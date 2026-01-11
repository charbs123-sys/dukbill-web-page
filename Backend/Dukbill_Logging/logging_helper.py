import json
import logging
from fastapi import Request

logger = logging.getLogger()

def log_event(
    request: Request,
    event: str,
    message: dict | None = None,
    level: str = "info",
):
    log_data = {
        "event": event,
        "request_id": getattr(request.state, "request_id", None),
        "method": request.method,
        "path": request.url.path,
        "user_id": request.headers.get("X-User-Id"),
        "ip": request.headers.get("X-Forwarded-For", request.client.host if request.client else None),
    }

    if message:
        log_data.update(message)

    log_fn = getattr(logger, level, logger.info)
    log_fn(json.dumps(log_data))
