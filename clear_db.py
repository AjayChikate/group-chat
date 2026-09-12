
import os
import sqlite3

def clear_database():
    # 1. Clear MongoDB messages
    try:
        from pymongo import MongoClient
        mongodb_url = os.environ.get(
            'MONGODB_URL',
            'mongodb+srv://ajaychikate55555_db_user:y9vwFLWk0Jk6QK3d@cluster0.ofpblfl.mongodb.net/'
        )
        mongodb_db = os.environ.get('MONGODB_DB', 'group_chat')
        client = MongoClient(mongodb_url, serverSelectionTimeoutMS=5000)
        db = client[mongodb_db]
        res = db['messages'].delete_many({})
        print(f"[*] MongoDB: Deleted {res.deleted_count} messages from '{mongodb_db}.messages'.")
    except Exception as e:
        print(f"[!] MongoDB clear failed or skipped: {e}")

    # 2. Clear SQLite database if present
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'chat.db')
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            with conn:
                msg_count = conn.execute("SELECT count(*) FROM messages").fetchone()[0]
                conn.execute("DELETE FROM messages")
                conn.execute("DELETE FROM user_keys")
            conn.execute("VACUUM")
            conn.close()
            print(f"[*] SQLite: Deleted {msg_count} message(s) from 'messages' table.")
        except Exception as e:
            print(f"[!] SQLite clear failed: {e}")

    print(">>> DATABASE CLEARED SUCCESSFULLY. Ready for fresh test run.")

if __name__ == '__main__':
    clear_database()
