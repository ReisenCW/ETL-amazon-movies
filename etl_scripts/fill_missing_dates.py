import sqlite3
import os
from datetime import datetime

def fill_missing_dates():
    db_path = os.path.join(os.path.dirname(__file__), '..', 'database', 'amazon_movies.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 为缺失release_date的电影，从评论中推断
    cursor.execute('''
        UPDATE final_movies
        SET release_date = (
            SELECT MIN(datetime(rr.time, 'unixepoch'))
            FROM raw_reviews rr
            INNER JOIN raw_products rp ON rr.product_id = rp.product_id
            WHERE rp.movie_name = final_movies.movie_name
        )
        WHERE release_date = '' OR release_date IS NULL
    ''')

    conn.commit()
    conn.close()
    print("Missing dates filled successfully")