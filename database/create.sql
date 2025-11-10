-- 1. 原始评价数据表（存储movies.txt解析结果）
CREATE TABLE raw_reviews (
    review_id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    profile_name TEXT,
    helpfulness TEXT,
    score REAL,
    time INTEGER, -- 保留unix时间戳（SQLite无BIGINT，INTEGER足够存储）
    summary TEXT,
    text TEXT,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- 索引优化：加速product_id关联查询
CREATE INDEX idx_raw_reviews_product_id ON raw_reviews(product_id);

-- 2. Product详情表（存储爬虫解析的电影元数据）
CREATE TABLE raw_products (
    product_id TEXT PRIMARY KEY,
    html_path TEXT NOT NULL, -- 原始HTML存储路径
    release_date TEXT, -- SQLite无DATE类型，用TEXT存储（格式：YYYY-MM-DD）
    genre TEXT,
    director TEXT,
    actors TEXT,
    version TEXT,
    crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_url TEXT, -- Amazon商品页URL
    movie_name TEXT -- 电影名（用于去重）
);

-- 3. 最终电影表（去重合并后）
CREATE TABLE final_movies (
    movie_id INTEGER PRIMARY KEY AUTOINCREMENT,
    core_product_id TEXT NOT NULL, -- 主版本ProductID
    movie_name TEXT NOT NULL, -- 从Product页提取电影名
    release_date TEXT, -- 格式：YYYY-MM-DD
    genre TEXT,
    director TEXT,
    actors TEXT,
    versions TEXT, -- 合并的所有版本（用逗号分隔）
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- 索引优化：加速电影去重分组
CREATE INDEX idx_final_movies_name_dir ON final_movies(movie_name, director);

-- 4. 评价关联表（最终电影与评价的关联）
CREATE TABLE final_movie_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER,
    user_id TEXT,
    profile_name TEXT,
    helpfulness TEXT,
    score REAL,
    review_time TEXT, -- 转换后的时间（格式：YYYY-MM-DD HH:MM:SS）
    summary TEXT,
    text TEXT,
    FOREIGN KEY (movie_id) REFERENCES final_movies(movie_id)
);
CREATE INDEX idx_final_reviews_movie_id ON final_movie_reviews(movie_id);

-- 5. 数据血缘表
CREATE TABLE data_lineage (
    lineage_id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_entity TEXT NOT NULL, -- 目标实体（final_movies/final_movie_reviews）
    target_field TEXT NOT NULL, -- 目标字段（如release_date/director）
    source_type TEXT NOT NULL, -- 来源类型（amazon_crawl/imdb/douban/review_time）
    source_id TEXT, -- 来源ID（如product_id/review_id）
    source_field TEXT, -- 来源字段
    merge_strategy TEXT, -- 合并策略（如取最早日期/取多数值）
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. 人名标准化表（存储别名映射）
CREATE TABLE name_normalization (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_name TEXT NOT NULL, -- 原始名字
    normalized_name TEXT NOT NULL, -- 标准化名字
    match_type TEXT, -- 匹配类型（middle_name/abbreviation/etc.）
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(raw_name) -- 避免重复别名
);