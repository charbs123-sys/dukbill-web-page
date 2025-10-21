import mysql.connector
import os

def initialize_database():
    """Initialize database and create it if it doesn't exist"""
    conn = None
    try:
        print("Connecting to MySQL server...")
        # Connect WITHOUT specifying a database
        conn = mysql.connector.connect(
            host=os.environ.get('DB_HOST'),
            port=int(os.environ.get('DB_PORT', 3306)),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD')
        )
        cursor = conn.cursor()
        
        # Create database if it doesn't exist
        db_name = os.environ.get('DB_NAME', 'dukbill')
        print(f"Creating database '{db_name}' if it doesn't exist...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
        cursor.execute(f"USE `{db_name}`")
        
        print(f"Successfully connected to database: {db_name}")
        
        # Test the connection
        cursor.execute("SELECT DATABASE()")
        current_db = cursor.fetchone()
        print(f"Current database: {current_db[0]}")
        
        # Create tables
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
        print("Users table created/verified")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS brokers (
                broker_id CHAR(6) PRIMARY KEY,
                user_id INT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)
        print("Brokers table created/verified")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                client_id CHAR(6) PRIMARY KEY,
                user_id INT NOT NULL,
                broker_id CHAR(6) NOT NULL,
                brokerAccess BOOLEAN NOT NULL DEFAULT FALSE,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (broker_id) REFERENCES brokers(broker_id)
            )
        """)
        print("Clients table created/verified")

        conn.commit()
        cursor.close()
        conn.close()
        print("Database initialization completed successfully")
        
    except mysql.connector.Error as err:
        print(f"Database initialization error: {err}")
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()