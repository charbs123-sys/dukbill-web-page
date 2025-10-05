import mysql.connector
import random
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

def retrieve_broker(user_id):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM brokers WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result

def retrieve_client(user_id):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM clients WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result

def generate_id(length=6):
    # Generates a numeric string, e.g., "348291"
    return ''.join(random.choices("0123456789", k=length))

def get_unique_broker_id():
    conn = get_connection()
    cursor = conn.cursor()
    broker_id = None

    while True:
        candidate = generate_id()
        cursor.execute("SELECT broker_id FROM brokers WHERE broker_id = %s", (candidate,))
        if cursor.fetchone() is None:
            broker_id = candidate
            break

    cursor.close()
    conn.close()
    return broker_id


def get_unique_client_id():
    conn = get_connection()
    cursor = conn.cursor()
    client_id = None

    while True:
        candidate = generate_id()
        cursor.execute("SELECT client_id FROM clients WHERE client_id = %s", (candidate,))
        if cursor.fetchone() is None:
            client_id = candidate
            break

    cursor.close()
    conn.close()
    return client_id

def verify_user_by_id(user_id):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result

def verify_broker_by_id(broker_id):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM brokers WHERE broker_id = %s", (broker_id,))
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

def add_broker(user_id):
    broker_id = get_unique_broker_id()
    
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "INSERT INTO brokers (user_id) VALUES (%s)",
        (broker_id, user_id,)
    )
    conn.commit()
    cursor.close()
    conn.close()
    return broker_id

def add_client(user_id, broker_id):
    client_id = get_unique_client_id()
    
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "INSERT INTO clients (client_id, user_id, broker_id) VALUES (%s, %s, %s)",
        (client_id, user_id, broker_id,)
    )
    conn.commit()
    cursor.close()
    conn.close()
    return client_id

def update_user_profile(auth0_id: str, profile_data: dict):
    conn = get_connection()
    cursor = conn.cursor()

    fields = []
    values = []

    if "user_type" in profile_data:
        user_type = profile_data.pop("user_type")
        if user_type == "client":
            fields.append("isBroker=%s")
            values.append(False)
        elif user_type == "broker":
            fields.append("isBroker=%s")
            values.append(True)

    for key, value in profile_data.items():
        if value is not None:
            fields.append(f"{key}=%s")
            values.append(value)

    if not fields:
        sql = "UPDATE users SET profile_complete = TRUE WHERE auth0_id=%s"
        cursor.execute(sql, (auth0_id,))
    else:
        fields.append("profile_complete = TRUE")
        values.append(auth0_id)
        sql = f"UPDATE users SET {', '.join(fields)} WHERE auth0_id=%s"
        cursor.execute(sql, values)

    conn.commit()
    cursor.close()
    conn.close()
    
def get_clients_for_broker(broker_id):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
    SELECT 
        c.client_id AS id,
        u.name,
        u.picture
    FROM clients c
    JOIN users u ON c.user_id = u.user_id
    WHERE c.broker_id = %s;
    """
    cursor.execute(query, (broker_id,))
    clients = cursor.fetchall()

    cursor.close()
    conn.close()
    return clients

