from db_utils import get_connection

def initialize_database():
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        auth0_id VARCHAR(255) UNIQUE NOT NULL,
        name VARCHAR(255),
        email VARCHAR(255) NOT NULL,
        phone VARCHAR(20),
        company VARCHAR(255),
        picture VARCHAR(255),
        profile_complete BOOLEAN NOT NULL DEFAULT FALSE
    )
    """)
    
    #cursor.execute("""
    #    CREATE TABLE IF NOT EXISTS clients (
    #    user_id INT PRIMARY KEY,
    #    permissions VARCHAR(255),
    #    FOREIGN KEY (user_id) REFERENCES users(id)
    #)
    #""")
    
    #cursor.execute("""
    #CREATE TABLE IF NOT EXISTS brokers (
    #    user_id INT PRIMARY KEY,
    #    full_name VARCHAR(255),
    #    address VARCHAR(255),
    #    FOREIGN KEY (user_id) REFERENCES users(id)
    #)
    #""")
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Database initialized safely.")