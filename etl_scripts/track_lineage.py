import sqlite3
import os
from datetime import datetime

def track_lineage():
    db_path = os.path.join(os.path.dirname(__file__), '..', 'database', 'amazon_movies.db')
    print(f"Database path: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 创建血缘表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_lineage (
            lineage_id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_entity TEXT NOT NULL,
            target_field TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_id TEXT,
            source_field TEXT,
            merge_strategy TEXT,
            load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 为final_movies记录血缘
    cursor.execute('SELECT movie_id, core_product_id FROM final_movies')
    for movie_id, core_product_id in cursor.fetchall():
        cursor.execute('''
            INSERT INTO data_lineage (target_entity, target_field, source_type, source_id, source_field, merge_strategy)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', ('final_movies', 'movie_name', 'amazon_crawl', core_product_id, 'movie_name', 'direct'))

        cursor.execute('''
            INSERT INTO data_lineage (target_entity, target_field, source_type, source_id, source_field, merge_strategy)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', ('final_movies', 'release_date', 'amazon_crawl', core_product_id, 'release_date', 'min_date'))

    # 为缺失日期记录从评论推断
    cursor.execute('''
        SELECT movie_id FROM final_movies
        WHERE release_date NOT IN (SELECT release_date FROM raw_products WHERE product_id = final_movies.core_product_id)
    ''')
    for (movie_id,) in cursor.fetchall():
        cursor.execute('''
            INSERT INTO data_lineage (target_entity, target_field, source_type, source_id, source_field, merge_strategy)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', ('final_movies', 'release_date', 'review_time', str(movie_id), 'time', 'min_review_time'))

    conn.commit()
    conn.close()
    print("Lineage tracked successfully")