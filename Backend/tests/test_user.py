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

    # Mock Auth0 userinfo call
    monkeypatch.setattr(dukbill, "get_user_info_from_auth0", lambda access_token: {
        "email_verified": True
    })

    r = await client.get("/user/profile")
    assert r.status_code == 200
    data = r.json()
    assert data["name"] == "Test User"
    assert data["id"] == 888
    assert data["picture"] == "https://example.com/p.png"
    assert data["user_type"] == "broker"
    assert data["email_verified"] is True
    
import pytest

@pytest.mark.asyncio
async def test_complete_profile_client(monkeypatch, client):
    import dukbill

    # Mock Auth0 & user lookups
    monkeypatch.setattr(dukbill, "get_current_user", lambda: ({"sub": "auth0|123"}, "token123"))
    monkeypatch.setattr(dukbill, "get_user_info_from_auth0", lambda token: {"sub": "auth0|123"})
    monkeypatch.setattr(dukbill, "search_user_by_auth0", lambda auth0_id: {
        "user_id": 1,
        "email": "client@example.com",
        "profile_complete": True,
    })
    monkeypatch.setattr(dukbill, "register_client", lambda user_id, broker_id: 10)
    monkeypatch.setattr(dukbill, "client_add_email", lambda client_id, domain, email: True)
    monkeypatch.setattr(dukbill, "update_profile", lambda auth0_id, data: {
        "user_id": 1,
        "profile_complete": True,
        "full_name": "John Doe",
        "phone_number": "123456789",
        "company_name": None
    })

    profile_data = {"user_type": "client", "broker_id": 5, "full_name": "John Doe", "phone_number": "123456789"}
    r = await client.patch("/users/onboarding", json=profile_data)

    assert r.status_code == 200
    data = r.json()
    assert data["user"] == 1
    assert data["profileComplete"] is True
    assert data["missingFields"] == ["company_name"]
    assert data["validatedBroker"] is True


@pytest.mark.asyncio
async def test_complete_profile_broker(monkeypatch, client):
    import dukbill

    # Mock Auth0 & user lookups
    monkeypatch.setattr(dukbill, "get_current_user", lambda: ({"sub": "auth0|456"}, "token456"))
    monkeypatch.setattr(dukbill, "get_user_info_from_auth0", lambda token: {"sub": "auth0|456"})
    monkeypatch.setattr(dukbill, "search_user_by_auth0", lambda auth0_id: {
        "user_id": 2,
        "email": "broker@example.com",
        "profile_complete": False,
    })
    monkeypatch.setattr(dukbill, "register_broker", lambda user_id: True)
    monkeypatch.setattr(dukbill, "update_profile", lambda auth0_id, data: {
        "user_id": 2,
        "profile_complete": True,
        "full_name": None,
        "phone_number": None,
        "company_name": "MyBrokerCo"
    })

    profile_data = {"user_type": "broker", "company_name": "MyBrokerCo"}
    r = await client.patch("/users/onboarding", json=profile_data)

    assert r.status_code == 200
    data = r.json()
    assert data["user"] == 2
    assert data["profileComplete"] is True
    assert data["missingFields"] == ["full_name", "phone_number"]
    assert data["validatedBroker"] is True

   