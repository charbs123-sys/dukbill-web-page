import mysql.connector
import os

def initialize_database():
    """Drop existing tables and recreate them"""
    conn = None
    try:
        print("Connecting to MySQL server...")
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
        
        # Drop tables in order: clients -> brokers -> users
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")  # disable foreign key checks
        cursor.execute("DROP TABLE IF EXISTS clients")
        cursor.execute("DROP TABLE IF EXISTS brokers")
        cursor.execute("DROP TABLE IF EXISTS users")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")  # enable foreign key checks
        print("Existing tables dropped")
        
        # Recreate tables
        cursor.execute("""
            CREATE TABLE users (
                user_id INT AUTO_INCREMENT PRIMARY KEY,
                auth0_id VARCHAR(255) UNIQUE NOT NULL,
                basiq_id VARCHAR(255),
                name VARCHAR(255),
                email VARCHAR(255) NOT NULL,
                phone VARCHAR(20),
                company VARCHAR(255),
                picture VARCHAR(255),
                isBroker BOOLEAN NOT NULL DEFAULT FALSE,
                profile_complete BOOLEAN NOT NULL DEFAULT FALSE,
                email_scan BOOLEAN NOT NULL DEFAULT FALSE
            )
        """)
        print("Users table created")

        cursor.execute("""
            CREATE TABLE brokers (
                broker_id CHAR(6) PRIMARY KEY,
                user_id INT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)
        print("Brokers table created")

        cursor.execute("""
            CREATE TABLE clients (
                client_id CHAR(6) PRIMARY KEY,
                user_id INT NOT NULL,
                broker_id CHAR(6) NOT NULL,
                brokerAccess BOOLEAN NOT NULL DEFAULT FALSE,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (broker_id) REFERENCES brokers(broker_id)
            )
        """)
        print("Clients table created")

        conn.commit()
        cursor.close()
        conn.close()
        print("Database recreated safely.")

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        if conn and conn.is_connected():
            conn.close()
        raise
