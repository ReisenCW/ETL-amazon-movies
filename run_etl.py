#!/usr/bin/env python3
"""
ETL 主脚本 - 顺序执行所有ETL任务
"""

import sys
import os

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from etl_scripts.clear_db import clear_db
from etl_scripts.load_reviews import load_reviews_to_db
from etl_scripts.load_products import load_products_to_db
from etl_scripts.merge_movies import merge_movies
from etl_scripts.normalize_names import normalize_names
from etl_scripts.fill_missing_dates import fill_missing_dates
from etl_scripts.track_lineage import track_lineage

def main():
    print("开始ETL流程...")
    is_clear_db = True
    is_load_reviews = True
    is_load_products = True
    is_merge_movies = True
    is_normalize_names = True
    is_fill_missing_dates = True
    is_track_lineage = True

    try:
        if is_clear_db:
            print("0. 清空数据库...")
            clear_db()

        if is_load_reviews:
            print("1. 加载评价数据...")
            load_reviews_to_db()

        if is_load_products:
            print("2. 加载产品数据...")
            load_products_to_db()

        if is_merge_movies:
            print("3. 合并电影...")
            merge_movies()

        if is_normalize_names:
            print("4. 标准化人名...")
            normalize_names()

        if is_fill_missing_dates:
            print("5. 填充缺失日期...")
            fill_missing_dates()

        if is_track_lineage:
            print("6. 跟踪数据血缘...")
            track_lineage()

        print("ETL流程完成！")

    except Exception as e:
        print(f"ETL过程中出错: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()