import duckdb
import pandas as pd
import os

DB_PATH = "pricehubble.db"
EXPORT_CSV = True  # Set to False if you only want to view

def main():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at: {DB_PATH}")
        return

    conn = duckdb.connect(DB_PATH)
    cursor = conn.cursor()

    print("Connected to database.\n")

    # List all tables
    tables = cursor.execute(
        "SHOW TABLES;"
    ).fetchall()
    tables = [t[0] for t in tables]
    print(f"Tables in the database: {tables}\n")

    for table in tables:
        print(f"Schema for table '{table}':")
        schema = cursor.execute(f"DESCRIBE {table};").fetchall()
        for col in schema:
            print(f"  - {col[0]} ({col[1]})")
        print()

        print(f"Sample rows from '{table}':")
        df = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 5;", conn)
        print(df.to_markdown(index=False))
        print("-" * 50 + "\n")

        if EXPORT_CSV:
            export_path = f"{table}.csv"
            df_full = pd.read_sql_query(f"SELECT * FROM {table};", conn)
            df_full = df_full.drop_duplicates()  #  remove duplicates
            df_full.to_csv(export_path, index=False)            
            print(f"Exported {len(df_full)} rows to: {export_path}")

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
