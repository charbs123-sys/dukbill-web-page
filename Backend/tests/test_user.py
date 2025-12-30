import pytest

@pytest.mark.asyncio
async def test_fetch_client_user_profile(monkeypatch, client):
    import dukbill

    # Mock DB/user lookups
    monkeypatch.setattr(dukbill, "find_user", lambda auth0_id: {
        "user_id": 123,
        "auth0_id": "auth0|test-user",
        "name": "Test User",
        "email": "test@example.com",
        "phone": "123-456-7890",
        "company": "Test Corp",
        "picture": "https://example.com/p.png",
        "isBroker": False,
        "profile_complete": True,
        "email": "client@example.com",
    })
    monkeypatch.setattr(dukbill, "find_client", lambda user_id: {"client_id": 999})
    monkeypatch.setattr(dukbill, "find_broker", lambda user_id: None)

    # Mock Auth0 userinfo call
    monkeypatch.setattr(dukbill, "get_user_info_from_auth0", lambda access_token: {
        "email_verified": True
    })

    r = await client.get("/user/profile")
    assert r.status_code == 200
    data = r.json()
    assert data["name"] == "Test User"
    assert data["id"] == 999
    assert data["picture"] == "https://example.com/p.png"
    assert data["user_type"] == "client"
    assert data["email_verified"] is True

@pytest.mark.asyncio
async def test_fetch_broker_user_profile(monkeypatch, client):
    import dukbill

    # Mock DB/user lookups
    monkeypatch.setattr(dukbill, "find_user", lambda auth0_id: {
        "user_id": 123,
        "auth0_id": "auth0|test-user",
        "name": "Test User",
        "email": "test@example.com",
        "phone": "123-456-7890",
        "company": "Test Corp",
        "picture": "https://example.com/p.png",
        "isBroker": True,
        "profile_complete": True,
        "email": "broker@example.com",
    })
    
    monkeypatch.setattr(dukbill, "find_client", lambda user_id: {"client_id": 999})
    monkeypatch.setattr(dukbill, "find_broker", lambda user_id: {"broker_id": 888})
    