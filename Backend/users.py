from db_utils import search_user_by_auth0, add_user, update_user_profile

def find_user(auth0_id):
    user = search_user_by_auth0(auth0_id)
    return user

def register_client(auth0_id, email, picture, profileComplete):
    # You could add any business logic here before inserting
    user_id = add_user(auth0_id, email, picture, profileComplete)
    return user_id

def update_profile(auth0_id: str, profile_data: dict):
    user = search_user_by_auth0(auth0_id)
    if not user:
        raise ValueError(f"User with auth0_id {auth0_id} not found")

    update_user_profile(auth0_id, profile_data)
    updated_user = search_user_by_auth0(auth0_id)
    return updated_user
