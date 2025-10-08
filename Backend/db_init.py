from db_utils import get_connection

def initialize_database():
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INT AUTO_INCREMENT PRIMARY KEY,
            auth0_id VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255),
            email VARCHAR(255) NOT NULL,
            phone VARCHAR(20),
            company VARCHAR(255),
            picture VARCHAR(255),
            isBroker BOOLEAN NOT NULL DEFAULT FALSE,
            profile_complete BOOLEAN NOT NULL DEFAULT FALSE
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS brokers (
            broker_id CHAR(6) PRIMARY KEY,
            user_id INT NOT NULL,
            brokerAccess BOOLEAN NOT NULL DEFAULT FALSE,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            client_id CHAR(6) PRIMARY KEY,
            user_id INT NOT NULL,
            broker_id CHAR(6) NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (broker_id) REFERENCES brokers(broker_id)
        )
    """)

    
    conn.commit()
    cursor.close()
    conn.close()
    print("Database initialized safely.")