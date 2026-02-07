import sqlite3
from werkzeug.security import generate_password_hash
from modules.db_logger import DB_PATH

def update_admin():
    username = "casabero"
    password = "casamix123"
    
    print(f"Connecting to DB at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Ensure table exists (just in case)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS admin_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Check if user exists
    cursor.execute("SELECT id FROM admin_users WHERE username=?", (username,))
    exists = cursor.fetchone()
    
    hashed_pw = generate_password_hash(password)
    
    if exists:
        print(f"Updating password for {username}...")
        cursor.execute("UPDATE admin_users SET password_hash=? WHERE username=?", (hashed_pw, username))
    else:
        print(f"Creating user {username}...")
        cursor.execute("INSERT INTO admin_users (username, password_hash) VALUES (?, ?)", (username, hashed_pw))
    
    conn.commit()
    conn.close()
    print("Done.")

if __name__ == "__main__":
    update_admin()
