import sqlite3
import os

def create_database():
    db_path = os.path.join(os.path.dirname(__file__), 'database', 'amazon_movies_etl.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 读取并执行create.sql
    sql_file = os.path.join(os.path.dirname(__file__), 'database', 'create.sql')
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_script = f.read()

    cursor.executescript(sql_script)
    conn.commit()
    conn.close()
    print("Database created successfully")

if __name__ == "__main__":
    create_database()