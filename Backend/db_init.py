import mysql.connector
import os

def initialize_database():
    """Initialize database and alter tables if needed"""
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
        
        db_name = os.environ.get('DB_NAME', 'dukbill')
        print(f"Creating database '{db_name}' if it doesn't exist...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
        cursor.execute(f"USE `{db_name}`")
        
        print(f"Successfully connected to database: {db_name}")

        # Helper function to check if a column exists
        def column_exists(table, column):
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{db_name}' 
                AND TABLE_NAME = '{table}' 
                AND COLUMN_NAME = '{column}'
            """)
            return cursor.fetchone()[0] > 0

        # ---- USERS TABLE ----
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS users (
                user_id INT AUTO_INCREMENT PRIMARY KEY
            )
        """)
        
        # Add missing columns
        users_columns = {
            "auth0_id": "VARCHAR(255) UNIQUE NOT NULL",
            "name": "VARCHAR(255)",
            "email": "VARCHAR(255) NOT NULL",
            "phone": "VARCHAR(20)",
            "company": "VARCHAR(255)",
            "picture": "VARCHAR(255)",
            "isBroker": "BOOLEAN NOT NULL DEFAULT FALSE",
            "profile_complete": "BOOLEAN NOT NULL DEFAULT FALSE"
        }

        for col, col_def in users_columns.items():
            if not column_exists("users", col):
                cursor.execute(f"ALTER TABLE users ADD COLUMN {col} {col_def}")
                print(f"Added column '{col}' to users table")

        # ---- BROKERS TABLE ----
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS brokers (
                broker_id CHAR(6) PRIMARY KEY
            )
        """)
        brokers_columns = {
            "user_id": "INT NOT NULL"
        }
        for col, col_def in brokers_columns.items():
            if not column_exists("brokers", col):
                cursor.execute(f"ALTER TABLE brokers ADD COLUMN {col} {col_def}")
                print(f"Added column '{col}' to brokers table")
        
        # Add foreign key if not exists
        try:
            cursor.execute("ALTER TABLE brokers ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(user_id)")
        except mysql.connector.Error:
            pass  # Foreign key may already exist

        # ---- CLIENTS TABLE ----
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS clients (
                client_id CHAR(6) PRIMARY KEY
            )
        """)
        clients_columns = {
            "user_id": "INT NOT NULL",
            "broker_id": "CHAR(6) NOT NULL",
            "brokerAccess": "BOOLEAN NOT NULL DEFAULT FALSE"
        }
        for col, col_def in clients_columns.items():
            if not column_exists("clients", col):
                cursor.execute(f"ALTER TABLE clients ADD COLUMN {col} {col_def}")
                print(f"Added column '{col}' to clients table")
        
        # Add foreign keys if not exists
        try:
            cursor.execute("ALTER TABLE clients ADD CONSTRAINT fk_client_user FOREIGN KEY (user_id) REFERENCES users(user_id)")
        except mysql.connector.Error:
            pass

        try:
            cursor.execute("ALTER TABLE clients ADD CONSTRAINT fk_client_broker FOREIGN KEY (broker_id) REFERENCES brokers(broker_id)")
        except mysql.connector.Error:
            pass

        conn.commit()
        cursor.close()
        conn.close()
        print("Database initialization and table alteration completed successfully")

    except mysql.connector.Error as err:
        print(f"Database initialization error: {err}")
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()
