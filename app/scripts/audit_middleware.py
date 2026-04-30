import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

_PATH_ACTION_MAP = [
    ("POST",   "/query",            "QUERY"),
    ("POST",   "/upload/",          "UPLOAD"),
    ("POST",   "/delete",           "DELETE"),
    ("POST",   "/update",           "UPDATE"),
    ("POST",   "/schemas",          "SCHEMA_CREATE"),
    ("DELETE", "/schemas/",         "SCHEMA_DELETE"),
    ("GET",    "/schemas",          "SCHEMA_READ"),
    ("POST",   "/access-policies",  "POLICY_CREATE"),
    ("DELETE", "/access-policies",  "POLICY_DELETE"),
    ("GET",    "/access-policies/", "POLICY_READ"),
    ("PUT",    "/index-cids",       "INDEX_UPDATE"),
    ("DELETE", "/index-cids",       "INDEX_DELETE"),
    ("POST",   "/tables/config",    "TABLE_CONFIG"),
    ("GET",    "/tables/config",    "TABLE_CONFIG"),
    ("GET",    "/ipfs/fetch/",      "IPFS_FETCH"),
    ("GET",    "/health",           "HEALTH_CHECK"),
    ("POST",   "/web3db/store",     "WEB3DB_STORE"),
    ("GET",    "/web3db/fetch/",    "WEB3DB_FETCH"),
    ("POST",   "/web3health/store", "WEB3HEALTH_STORE"),
    ("GET",    "/web3health/fetch/","WEB3HEALTH_FETCH"),
]


def _resolve_action(method: str, path: str) -> str:
    for m, p, action in _PATH_ACTION_MAP:
        if method == m and path.startswith(p):
            return action
    return "UNKNOWN"


class AuditMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, audit_logger):
        super().__init__(app)
        self.audit_logger = audit_logger

    async def dispatch(self, request: Request, call_next) -> Response:
        request.state.audit = {}
        ip = request.client.host if request.client else None

        response = await call_next(request)

        audit_ctx = request.state.audit
        if "action" not in audit_ctx:
            audit_ctx["action"] = _resolve_action(request.method, request.url.path)

        if response.status_code in (401, 403):
            status = "UNAUTHORIZED"
        elif response.status_code >= 400:
            status = "ERROR"
        else:
            status = "SUCCESS"

        try:
            self.audit_logger.log(
                audit_ctx=audit_ctx,
                api_endpoint=request.url.path,
                ip_address=ip,
                status=status,
            )
        except Exception as e:
            logger.error(f"Audit logging failed: {e}")

        return response
