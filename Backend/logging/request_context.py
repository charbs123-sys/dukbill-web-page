import time
import uuid
from fastapi import Request

def register_request_context(app):

    @app.middleware("http")
    async def request_context_middleware(request: Request, call_next):
        request.state.start_time = time.time()
        request.state.request_id = str(uuid.uuid4())

        response = await call_next(request)
        return response
