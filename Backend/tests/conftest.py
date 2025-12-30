import os
import sys
from pathlib import Path
import pytest
import pytest_asyncio
from httpx import AsyncClient
from httpx._transports.asgi import ASGITransport

# Set test env early
os.environ["ENV"] = "test"

# Add Backend/ to Python path so `import dukbill` works
BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

@pytest.fixture
def app():
    import dukbill

    def test_current_user():
        claims = {
            "sub": "auth0|test-user",
            "email": "test@example.com",
            "email_verified": True,
            "name": "Test User",
            "given_name": "Test",
            "nickname": "testuser",
            "picture": "http://example.com/picture.jpg",
            "locale": "en-US",
            "updated_at": "2023-01-01T00:00:00Z"
        }
        token = "test.jwt.token"
        return claims, token

    dukbill.app.dependency_overrides[dukbill.get_current_user] = test_current_user
    return dukbill.app

@pytest_asyncio.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
