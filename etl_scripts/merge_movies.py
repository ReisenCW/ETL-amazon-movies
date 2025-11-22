import sqlite3
import os
from datetime import datetime
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import logging

# 停用词列表
STOP_WORDS = set(['a', 'an', 'the', 'at', 'in', 'on', 'of', 'for', 'to', 'with', 'by', 'and', 'or', 'but', 'as', 'is', 'it', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might', 'must', 'can', 'shall', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'we', 'they', 'me', 'him', 'her', 'us', 'them'])

def preprocess_title(title):
    """预处理电影名：小写，移除停用词"""
    if not title:
        return ""
    words = title.lower().split()
    filtered = [word for word in words if word not in STOP_WORDS]
    return ' '.join(filtered)

def merge_movies():
    logging.info("Starting merge_movies")
    db_path = os.path.join(os.path.dirname(__file__), '..', 'database', 'amazon_movies.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 创建最终表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS final_movies (
            movie_id INTEGER PRIMARY KEY AUTOINCREMENT,
            core_product_id TEXT NOT NULL,
            movie_name TEXT NOT NULL,
            release_date TEXT,
            genre TEXT,
            director TEXT,
            actors TEXT,
            versions TEXT,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_final_movies_name_dir ON final_movies(movie_name, director)')

    # 加载模型
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # 获取所有电影数据，按director分组
    cursor.execute('SELECT movie_name, director, product_id, release_date, genre, actors, version FROM raw_products WHERE movie_name != ""')
    rows = cursor.fetchall()

    # 按director分组
    director_groups = {}
    for row in rows:
        movie_name, director, product_id, release_date, genre, actors, version = row
        if director not in director_groups:
            director_groups[director] = []
        director_groups[director].append({
            'movie_name': movie_name,
            'product_id': product_id,
            'release_date': release_date,
            'genre': genre,
            'actors': actors,
            'version': version
        })

    # 处理每个director组
    for director, movies in director_groups.items():
        if len(movies) == 1:
            # 只有一个，直接插入
            movie = movies[0]
            cursor.execute('''
                INSERT INTO final_movies (core_product_id, movie_name, release_date, genre, director, actors, versions)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (movie['product_id'], movie['movie_name'], movie['release_date'], movie['genre'], director, movie['actors'], movie['version']))
            continue

        # 预处理标题
        titles = [preprocess_title(m['movie_name']) for m in movies]
        # 生成embeddings
        embeddings = model.encode(titles)
        # 计算相似度矩阵
        sim_matrix = cosine_similarity(embeddings)

        # 合并相似电影
        merged_groups = []
        visited = set()
        for i in range(len(movies)):
            if i in visited:
                continue
            group = [i]
            for j in range(i+1, len(movies)):
                if sim_matrix[i][j] > 0.8:  # 相似度阈值
                    group.append(j)
                    visited.add(j)
            merged_groups.append(group)

        # 为每个合并组插入一条记录
        for group in merged_groups:
            # 选择组中第一个作为代表
            rep_idx = group[0]
            rep_movie = movies[rep_idx]
            # 合并product_ids
            product_ids = [movies[idx]['product_id'] for idx in group]
            versions = ','.join([movies[idx]['version'] for idx in group if movies[idx]['version']])
            # 取最早release_date
            release_dates = [movies[idx]['release_date'] for idx in group if movies[idx]['release_date']]
            release_date = min(release_dates) if release_dates else ''
            # 取genre（假设相同）
            genre = rep_movie['genre']
            actors = rep_movie['actors']

            cursor.execute('''
                INSERT INTO final_movies (core_product_id, movie_name, release_date, genre, director, actors, versions)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (rep_movie['product_id'], rep_movie['movie_name'], release_date, genre, director, actors, versions))

    conn.commit()
    conn.close()
    logging.info("Finished merge_movies")

if __name__ == "__main__":
    film1 = "The Great Adventure"
    film2 = "Great an adventure"
    pre1 = preprocess_title(film1)
    pre2 = preprocess_title(film2)
    model = SentenceTransformer('all-MiniLM-L6-v2')
    emb1 = model.encode([pre1])
    emb2 = model.encode([pre2])
    similarity = cosine_similarity(emb1, emb2)[0][0]
    print(f"Similarity between '{film1}' and '{film2}': {similarity:.4f}")