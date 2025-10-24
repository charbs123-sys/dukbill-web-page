# file: db/init_db.py
import os
import mysql.connector


def initialize_database() -> None:
    """
    Create the database and tables only if they do not already exist.
    Idempotent: re-running does not drop or recreate existing tables.
    Requires env vars: DB_HOST, DB_PORT (opt), DB_USER, DB_PASSWORD, DB_NAME (opt).
    """
    conn = None
    try:
        print("Connecting to MySQL server...")
        conn = mysql.connector.connect(
            host=os.environ.get("DB_HOST"),
            port=int(os.environ.get("DB_PORT", 3306)),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            autocommit=False,  # ensure transactional DDL where supported
        )

        with conn.cursor() as cursor:
            db_name = os.environ.get("DB_NAME", "dukbill")

            # Ensure schema exists, then switch into it
            print(f"Ensuring database '{db_name}' exists...")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
            cursor.execute(f"USE `{db_name}`")
            print(f"Using database: {db_name}")

            # Create tables only if they don't exist (order matters for FKs)
            print("Ensuring 'users' table exists...")
            cursor.execute(
                """
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
                    profile_complete BOOLEAN NOT NULL DEFAULT FALSE,
                    email_scan BOOLEAN NOT NULL DEFAULT FALSE
                ) ENGINE=InnoDB
                """
            )

            print("Ensuring 'brokers' table exists...")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS brokers (
                    broker_id CHAR(6) PRIMARY KEY,
                    user_id INT NOT NULL,
                    CONSTRAINT fk_brokers_user
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                ) ENGINE=InnoDB
                """
            )

            print("Ensuring 'clients' table exists...")
            cursor.execute(
                """
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
                """
            )

        conn.commit()
        print("Schema ensured. No tables were dropped or recreated.")

    except mysql.connector.Error as err:
        # Why: make failures explicit so deployment can fail-fast
        print(f"Database error: {err}")
        if conn and conn.is_connected():
            conn.rollback()
            conn.close()
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()
