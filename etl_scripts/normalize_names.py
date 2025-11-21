import sqlite3
import os
from datetime import datetime
from fuzzywuzzy import fuzz
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

# eg. "Dr. John A. Smith Jr." -> "John Smith"
def normalize_name(name):
    """
    标准化人名：去除头衔、中间名、后缀，处理前缀，转换为Title Case
    """
    if not name:
        return name
    # 移除头衔
    titles = ['dr.', 'prof.', 'mr.', 'mrs.', 'ms.', 'miss', 'sir', 'madam', 'rev.', 'capt.', 'col.', 'maj.', 'lt.', 'sgt.']
    for title in titles:
        if name.lower().startswith(title + ' '):
            name = name[len(title)+1:]
    # 移除后缀
    suffixes = ['jr.', 'sr.', 'iii', 'ii', 'iv', 'phd', 'md', 'esq.', 'jr', 'sr']
    for suffix in suffixes:
        if name.lower().endswith(' ' + suffix):
            name = name[:-len(' ' + suffix)]
    # 分割
    parts = name.split()
    # 移除中间名（单个字母或常见中间名）
    filtered = []
    for part in parts:
        if len(part) == 1 and part.isalpha():
            continue  # 移除单个字母
        if len(part) == 2 and part[1] == '.' and part[0].isalpha():
            continue  # 移除如A.的缩写
        if part.lower() in ['van', 'de', 'la', 'le', 'du', 'von']:  # 保留前缀
            filtered.append(part.lower())
        else:
            filtered.append(part)
    if len(filtered) >= 2:
        # 取第一个和最后一个
        normalized = f"{filtered[0].title()} {filtered[-1].title()}"
    else:
        normalized = name.strip().title()
    return normalized

def find_similar_names(names, threshold=80):
    """
    使用fuzzy matching找到相似名字
    """
    model = SentenceTransformer('all-MiniLM-L6-v2')
    embeddings = model.encode(names)
    sim_matrix = cosine_similarity(embeddings)
    
    groups = []
    visited = set()
    for i in range(len(names)):
        if i in visited:
            continue
        group = [i]
        for j in range(i+1, len(names)):
            if sim_matrix[i][j] > 0.8 or fuzz.token_sort_ratio(names[i], names[j]) > threshold:
                group.append(j)
                visited.add(j)
        groups.append(group)
    return groups

def normalize_names():
    db_path = os.path.join(os.path.dirname(__file__), '..', 'database', 'amazon_movies.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 创建人名标准化表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS name_normalization (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            match_type TEXT,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(raw_name)
        )
    ''')

    # 获取所有导演和演员名字
    names = set()
    cursor.execute('SELECT DISTINCT director FROM final_movies WHERE director != ""')
    directors = cursor.fetchall()
    for (director,) in directors:
        names.add(director)

    cursor.execute('SELECT DISTINCT actors FROM final_movies WHERE actors != ""')
    actors_rows = cursor.fetchall()
    for (actors,) in actors_rows:
        if actors:
            for actor in actors.split(','):
                names.add(actor.strip())

    names = list(names)
    if not names:
        print("No names to normalize")
        return

    # 找到相似组
    groups = find_similar_names(names)

    # 为每个组选择代表名字
    for group in groups:
        rep_name = names[group[0]]  # 第一个作为代表
        normalized = normalize_name(rep_name)
        for idx in group:
            raw = names[idx]
            cursor.execute('''
                INSERT OR IGNORE INTO name_normalization (raw_name, normalized_name, match_type)
                VALUES (?, ?, ?)
            ''', (raw, normalized, 'fuzzy_match'))

    # 更新final_movies中的director
    cursor.execute('''
        UPDATE final_movies
        SET director = (SELECT normalized_name FROM name_normalization WHERE raw_name = final_movies.director)
        WHERE director IN (SELECT raw_name FROM name_normalization)
    ''')

    # 更新final_movies中的actors（逗号分隔）
    cursor.execute('SELECT movie_id, actors FROM final_movies WHERE actors != ""')
    for movie_id, actors in cursor.fetchall():
        normalized_actors = []
        for actor in actors.split(','):
            actor = actor.strip()
            cursor.execute('SELECT normalized_name FROM name_normalization WHERE raw_name = ?', (actor,))
            result = cursor.fetchone()
            normalized_actors.append(result[0] if result else normalize_name(actor))
        new_actors = ', '.join(normalized_actors)
        cursor.execute('UPDATE final_movies SET actors = ? WHERE movie_id = ?', (new_actors, movie_id))

    conn.commit()
    conn.close()
    print("Names normalized successfully with fuzzy matching")

if __name__ == "__main__":
    name1 = "Dr. John A. Smith Jr."
    name2 = "John Smith"
    norm1 = normalize_name(name1)
    norm2 = normalize_name(name2)
    print(f"Normalized '{name1}' to '{norm1}'")
    print(f"Normalized '{name2}' to '{norm2}'")
    similarity = fuzz.token_sort_ratio(norm1, norm2)
    print(f"Fuzzy similarity between '{norm1}' and '{norm2}': {similarity}")