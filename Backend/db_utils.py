import mysql.connector
from config import DB_CONFIG

def get_connection():
    return mysql.connector.connect(**DB_CONFIG)

def search_user_by_auth0(auth0_id):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE auth0_id = %s", (auth0_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result

def add_user(auth0_id, email, picture, profileComplete=False):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "INSERT INTO users (auth0_id, email, picture, profile_complete) VALUES (%s, %s, %s, %s)",
        (auth0_id, email, picture, profileComplete)
    )
    conn.commit()
    user_id = cursor.lastrowid
    cursor.close()
    conn.close()
    return user_id

def update_user_profile(auth0_id: str, profile_data: dict):
    conn = get_connection()
    cursor = conn.cursor()

    fields = []
    values = []

    for key, value in profile_data.items():
        if value is not None:
            fields.append(f"{key}=%s")
            values.append(value)

    if not fields:
        sql = "UPDATE users SET profile_complete = TRUE WHERE auth0_id=%s"
        cursor.execute(sql, (auth0_id,))
    else:
        fields.append("profile_complete = TRUE")
        values.append(auth0_id)  # for WHERE clause
        sql = f"UPDATE users SET {', '.join(fields)} WHERE auth0_id=%s"
        cursor.execute(sql, values)

    conn.commit()
    cursor.close()
    conn.close()
    

