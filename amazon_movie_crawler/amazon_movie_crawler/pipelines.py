# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import os
import sqlite3
from scrapy.exceptions import DropItem

class SQLitePipeline:
    def open_spider(self, spider):
        # 连接SQLite数据库（文件路径为项目根目录的amazon_movie_etl.db）
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))
        self.crawler_root = os.path.abspath(os.path.join(script_dir, '..'))
        db_path = os.path.join(self.project_root, 'database', 'amazon_movie_etl.db')
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        # 启用外键约束（SQLite默认关闭）
        self.cursor.execute("PRAGMA foreign_keys = ON")

    def close_spider(self, spider):
        self.conn.close()

    def process_item(self, item, spider):
        try:
            # 插入或更新raw_products表（SQLite用REPLACE INTO实现UPSERT）
            sql = """
            REPLACE INTO raw_products (product_id, source_url, html_path, release_date, genre, director, actors, version, movie_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            html_path = item['html_path']
            if not os.path.isabs(html_path):
                html_abs = os.path.abspath(os.path.join(self.crawler_root, html_path))
            else:
                html_abs = html_path
            stored_html_path = os.path.relpath(html_abs, start=self.project_root)

            self.cursor.execute(sql, (
                item['product_id'],
                item['source_url'],
                stored_html_path,
                item['release_date'] if item['release_date'] else None,
                item['genre'],
                item['director'],
                item['actors'],
                item['version'],
                item['movie_name']
            ))
            self.conn.commit()
            return item
        except Exception as e:
            self.conn.rollback()
            raise DropItem(f"存储失败：{str(e)}")