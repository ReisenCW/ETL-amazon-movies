import sqlite3
import os
from bs4 import BeautifulSoup
from datetime import datetime
import logging

def load_products_to_db():
    logging.info("Starting load_products_to_db")
    # 连接数据库
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..'))
    db_path = os.path.join(project_root, 'database', 'amazon_movies.db')
    print(f"Database path: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 创建表（如果不存在）
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS raw_products (
            product_id TEXT PRIMARY KEY,
            html_path TEXT NOT NULL,
            release_date TEXT,
            genre TEXT,
            director TEXT,
            actors TEXT,
            version TEXT,
            crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_url TEXT,
            movie_name TEXT
        )
    ''')

    # 解析HTML文件
    html_dir = os.path.join(project_root, 'data', 'crawled_html')
    try:
        filenames = os.listdir(html_dir)
    except FileNotFoundError:
        logging.warning(f"Directory {html_dir} not found. Skipping load_products_to_db.")
        return
    for filename in filenames:
        if filename.endswith('.html'):
            product_id = filename[:-5]  # 去掉.html
            html_path_abs = os.path.join(html_dir, filename)
            with open(html_path_abs, 'r', encoding='utf-8') as f:
                html_content = f.read()

            soup = BeautifulSoup(html_content, 'html.parser')

            # 检查是否是电影
            if not soup.find('div', id='imdbInfo_feature_div'):
                continue

            movie_name = soup.find(id='productTitle')
            movie_name = movie_name.get_text().strip() if movie_name else ''

            release_date = soup.find('span', string=lambda text: text and 'Release date' in text)
            if release_date:
                release_date = release_date.find_next('span').get_text().strip()
            else:
                release_date = ''

            genre = soup.find('span', string=lambda text: text and 'Genre' in text)
            if genre:
                genre = genre.find_next('span').get_text().strip()
            else:
                genre = ''

            p_details = soup.find('div', id='detailBullets_feature_div')
            director = ''
            actors = ''
            if p_details:
                # 解析详情列表
                bullet_items = p_details.select('ul.a-unordered-list li')
                for li in bullet_items:
                    label = li.find('span', class_='a-text-bold')
                    if not label:
                        continue

                    label_text = label.get_text(separator=' ', strip=True)
                    label_text = label_text.replace('\u00a0', ' ').replace('\u200e', '').replace(':', '').strip().lower()
                    value_span = label.find_next_sibling('span')
                    value_text = value_span.get_text(strip=True) if value_span else ''

                    if 'director' in label_text:
                        director = value_text
                    elif 'actors' in label_text or 'starring' in label_text:
                        actors = value_text

            # Fallback: 部分页面使用详情表格
            if not director or not actors:
                tables = soup.select('#productDetails_detailBullets_sections1 tr')
                for row in tables:
                    header = row.find('th')
                    value_td = row.find('td')
                    if not header or not value_td:
                        continue
                    header_text = header.get_text(separator=' ', strip=True).replace(':', '').strip().lower()
                    value_text = value_td.get_text(separator=' ', strip=True)
                    if not director and 'director' in header_text:
                        director = value_text
                    elif not actors and ('actors' in header_text or 'starring' in header_text):
                        actors = value_text

            # 版本（如DVD/蓝光）
            version_spans = soup.select('#tmmSwatches ul a span[aria-label]')
            version = ','.join(span.get_text().strip() for span in version_spans if span.get_text().strip())

            source_url = f'https://www.amazon.com/dp/{product_id}'

            stored_html_path = os.path.relpath(html_path_abs, start=project_root)

            cursor.execute('''
                INSERT OR REPLACE INTO raw_products (product_id, html_path, release_date, genre, director, actors, version, source_url, movie_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (product_id, stored_html_path, release_date, genre, director, actors, version, source_url, movie_name))

    conn.commit()
    conn.close()
    logging.info("Finished load_products_to_db")