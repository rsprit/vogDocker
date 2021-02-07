from starlette.middleware.base import BaseHTTPMiddleware
from ..database import SessionLocal


class DbSessionMiddleware(BaseHTTPMiddleware):

    async def dispatch(self, request, call_next):
        request.state.db = SessionLocal()
        try:
            return await call_next(request)
        finally:
            request.state.db.close()
