import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'trading_bot.db')


def inspect_database(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    if not tables:
        print("No tables found in the database.")
        return

    for (table_name,) in tables:
        print(f"\n=== Table: {table_name} ===")

        # Get table columns
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = [col[1] for col in cursor.fetchall()]
        print("Columns:", columns)

        # Get all rows
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()

        if rows:
            for row in rows:
                print(row)
        else:
            print("(No rows)")

    conn.close()


if __name__ == "__main__":
    inspect_database(DB_PATH)
