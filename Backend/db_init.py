# file: db/init_db.py
import os
import mysql.connector


def initialize_database() -> None:
    """
    Initializes the MySQL database and tables for Dukbill.
    Drops existing tables temporarily for a clean reset.
    """
    conn = None
    try:
        print("Connecting to MySQL server...")
        conn = mysql.connector.connect(
            host=os.environ.get("DB_HOST"),
            port=int(os.environ.get("DB_PORT", 3306)),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            autocommit=False,
        )

        with conn.cursor() as cursor:
            db_name = os.environ.get("DB_NAME", "dukbill")

            # Create database if it doesn't exist
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
            cursor.execute(f"USE `{db_name}`")

            # TEMP: drop tables for clean reset
            print("Deleting existing tables...")
            cursor.execute("DROP TABLE IF EXISTS emails")
            cursor.execute("DROP TABLE IF EXISTS clients")
            cursor.execute("DROP TABLE IF EXISTS brokers")
            cursor.execute("DROP TABLE IF EXISTS users")
            print("Tables deleted.")

            # Create users table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INT AUTO_INCREMENT PRIMARY KEY,
                    auth0_id VARCHAR(255) UNIQUE NOT NULL,
                    basiq_id VARCHAR(255),
                    name VARCHAR(255),
                    email VARCHAR(255) NOT NULL,
                    phone VARCHAR(20),
                    company VARCHAR(255),
                    picture VARCHAR(255),
                    isBroker BOOLEAN NOT NULL DEFAULT FALSE,
                    profile_complete BOOLEAN NOT NULL DEFAULT FALSE
                ) ENGINE=InnoDB
            """)

            # Create brokers table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS brokers (
                    broker_id CHAR(6) PRIMARY KEY,
                    user_id INT NOT NULL,
                    CONSTRAINT fk_brokers_user
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                ) ENGINE=InnoDB
            """)

            # Create clients table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS clients (
                    client_id CHAR(6) PRIMARY KEY,
                    user_id INT NOT NULL,
                    broker_id CHAR(6) NOT NULL,
                    brokerAccess BOOLEAN NOT NULL DEFAULT FALSE,
                    CONSTRAINT fk_clients_user
                        FOREIGN KEY (user_id) REFERENCES users(user_id),
                    CONSTRAINT fk_clients_broker
                        FOREIGN KEY (broker_id) REFERENCES brokers(broker_id)
                ) ENGINE=InnoDB
            """)

            # Create client_emails table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS emails (
                    email_id INT AUTO_INCREMENT PRIMARY KEY,
                    client_id CHAR(6) NOT NULL,
                    domain VARCHAR(255),
                    email_address VARCHAR(255),
                    CONSTRAINT fk_client_emails_client
                        FOREIGN KEY (client_id) REFERENCES clients(client_id)
                ) ENGINE=InnoDB
            """)

        conn.commit()
        print("Database initialized successfully.")

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        if conn and conn.is_connected():
            conn.rollback()
        raise

    finally:
        if conn and conn.is_connected():
            conn.close()
