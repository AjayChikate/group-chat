
import os
import sqlite3
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'chat.db')
def tamper_last_message():
    if not os.path.exists(DB_PATH):
        print(f"[!] Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    # Fetch the most recent message
    row = conn.execute("""
        SELECT id, room_id, sender, ciphertext, timestamp 
        FROM messages 
        ORDER BY timestamp DESC 
        LIMIT 1
    """).fetchone()

    if not row:
        print("[!] No messages found in the database to tamper with.")
        conn.close()
        return

    msg_id = row['id']
    sender = row['sender']
    room = row['room_id']
    raw_val = row['ciphertext']

    # Convert hex string or bytes to bytearray
    is_hex = isinstance(raw_val, str)
    raw_ct = bytearray(bytes.fromhex(raw_val) if is_hex else raw_val)
    
    old_byte = raw_ct[0]
    # Flip the lowest bit of the first byte
    raw_ct[0] ^= 0x01
    new_byte = raw_ct[0]

    # Save back to SQLite
    tampered_val = raw_ct.hex() if is_hex else bytes(raw_ct)
    conn.execute("UPDATE messages SET ciphertext = ? WHERE id = ?", (tampered_val, msg_id))
    conn.commit()
    conn.close()
    print(" TAMPERED LAST MESSAGE IN DATABASE")

    print(f" Message ID:  {msg_id}")
    print(f" Sender:      {sender}")
    print(f" Room:        #{room}")
    print(f" Flipped Bit: Byte[0] 0x{old_byte:02x} -> 0x{new_byte:02x}")
   
    print("Now refresh your chat window or reconnect to see tamper detection!")
if __name__ == '__main__':
    tamper_last_message()
