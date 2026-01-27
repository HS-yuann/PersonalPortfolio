"""
Query script for enhanced crypto insights database.
"""
import sqlite3
from datetime import datetime

def query_database():
    conn = sqlite3.connect('enhanced_crypto_insights.db')
    cursor = conn.cursor()

    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    print("=== ENHANCED CRYPTO INSIGHTS DATABASE ===\n")
    print(f"Database: enhanced_crypto_insights.db")
    print(f"Query time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    print(f"Tables found: {len(tables)}")
    for table in tables:
        print(f"  - {table[0]}")

    print("\n" + "="*60 + "\n")

    # Query each table
    for table in tables:
        table_name = table[0]
        print(f"\n### TABLE: {table_name} ###\n")

        # Get column info
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        print(f"Columns: {', '.join([col[1] for col in columns])}")

        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        count = cursor.fetchone()[0]
        print(f"Total rows: {count}")

        # Show last 5 rows if any
        if count > 0:
            cursor.execute(f"SELECT * FROM {table_name} ORDER BY rowid DESC LIMIT 5;")
            rows = cursor.fetchall()
            print(f"\nLast {len(rows)} rows:")
            for i, row in enumerate(rows, 1):
                print(f"\n  Row {i}:")
                for j, col in enumerate(columns):
                    col_name = col[1]
                    value = row[j]
                    # Truncate long values
                    if isinstance(value, str) and len(value) > 100:
                        value = value[:100] + "..."
                    print(f"    {col_name}: {value}")
        else:
            print("  (empty table)")

        print("\n" + "-"*60)

    conn.close()

if __name__ == "__main__":
    query_database()
