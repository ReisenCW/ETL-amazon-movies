import sqlite3
import os
from datetime import datetime
from bs4 import BeautifulSoup

def load_reviews_to_db():
    # 连接数据库
    db_path = os.path.join(os.path.dirname(__file__), '..', 'database', 'amazon_movies.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 创建表（如果不存在）
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS raw_reviews (
            review_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT NOT NULL,
            user_id TEXT,  -- 从HTML中可能没有user_id，用profile_name代替
            profile_name TEXT,
            helpfulness TEXT,
            score REAL,
            time TEXT,  -- HTML中的日期是字符串
            summary TEXT,
            text TEXT,
            load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_raw_reviews_product_id ON raw_reviews(product_id)')

    # 解析HTML文件中的reviews
    html_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'crawled_html')
    for filename in os.listdir(html_dir):
        if filename.endswith('.html'):
            product_id = filename[:-5]  # 去掉.html
            html_path = os.path.join(html_dir, filename)
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()

            soup = BeautifulSoup(html_content, 'html.parser')

            # 查找review块
            review_block = soup.find('h3', {'data-hook': 'dp-local-reviews-header'})
            if not review_block:
                continue
            review_container = review_block.find_parent().find_parent().find('div', class_=lambda x: x and 'a-row' in x if x else False)
            if not review_container:
                continue
            ul = review_container.find('ul')
            if not ul:
                continue

            count = 0
            for review_li in ul.find_all('li', {'data-hook': 'review'}):
                if count >= 5:  # 每个电影最多5条review
                    break

                profile_name = review_li.find('span', class_='a-profile-name')
                profile_name = profile_name.get_text().strip() if profile_name else ''

                rating_span = review_li.find('i', {'data-hook': 'review-star-rating'}).find('span') if review_li.find('i', {'data-hook': 'review-star-rating'}) else None
                score = 0.0
                if rating_span:
                    rating_text = rating_span.get_text().strip()
                    score = float(rating_text.split(' out ')[0]) if ' out ' in rating_text else 0.0

                review_title = review_li.find('a', {'data-hook': 'review-title'})
                summary = ''
                if review_title:
                    title_span = review_title.find('span')
                    summary = title_span.get_text().strip() if title_span else ''

                review_date_span = review_li.find('span', {'data-hook': 'review-date'})
                time = ''
                if review_date_span:
                    date_text = review_date_span.get_text().strip()
                    if ' on ' in date_text:
                        time = date_text.split(' on ')[-1]

                review_body = review_li.find('span', {'data-hook': 'review-body'})
                text = ''
                if review_body:
                    text = ' '.join(review_body.find('div').get_text().strip() for div in review_body.find_all('div') if div.get_text().strip())

                helpful_span = review_li.find('span', {'data-hook': 'helpful-vote-statement'})
                helpfulness = '0'
                if helpful_span:
                    help_text = helpful_span.get_text().strip().replace('One person', '1 people')
                    if ' people' in help_text:
                        helpfulness = help_text.split(' people')[0]
                    elif help_text.isdigit():
                        helpfulness = help_text

                # 插入review
                cursor.execute('''
                    INSERT INTO raw_reviews (product_id, user_id, profile_name, helpfulness, score, time, summary, text)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (product_id, profile_name, profile_name, helpfulness, score, time, summary, text))

                count += 1

    conn.commit()
    conn.close()
    print("Reviews loaded successfully from HTML")