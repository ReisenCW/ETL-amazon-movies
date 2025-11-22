from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import sys
import os

# 添加项目路径
current_file = os.path.abspath(__file__)
# 获取 DAG 文件所在的目录(airflow/dags)
dags_folder = os.path.dirname(current_file)
# 获取项目根目录(向上返回两级：airflow/dags → airflow → 项目根目录)
project_root = os.path.abspath(os.path.join(dags_folder, "..", ".."))
# 将项目根目录添加到 Python 搜索路径
sys.path.append(project_root)

from etl_scripts.load_reviews import load_reviews_to_db
from etl_scripts.load_products import load_products_to_db
from etl_scripts.merge_movies import merge_movies
from etl_scripts.normalize_names import normalize_names
from etl_scripts.fill_missing_dates import fill_missing_dates
from etl_scripts.track_lineage import track_lineage

default_args = {
    'owner': 'etl_user',
    'email': ['814568436@qq.com'],
    'depends_on_past': True,
    'start_date': datetime(2025, 11, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_amazon_movies',
    default_args=default_args,
    description='ETL pipeline for Amazon movies data',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# 任务1: 加载评价数据
load_reviews_task = PythonOperator(
    task_id='load_reviews',
    python_callable=load_reviews_to_db,
    dag=dag,
)

# 任务2: 加载产品数据
load_products_task = PythonOperator(
    task_id='load_products',
    python_callable=load_products_to_db,
    dag=dag,
)

# 任务3: 合并电影
merge_movies_task = PythonOperator(
    task_id='merge_movies',
    python_callable=merge_movies,
    dag=dag,
)

# 任务4: 标准化人名
normalize_names_task = PythonOperator(
    task_id='normalize_names',
    python_callable=normalize_names,
    dag=dag,
)

# 任务5: 填充缺失日期
fill_missing_dates_task = PythonOperator(
    task_id='fill_missing_dates',
    python_callable=fill_missing_dates,
    dag=dag,
)

# 任务6: 跟踪血缘
track_lineage_task = PythonOperator(
    task_id='track_lineage',
    python_callable=track_lineage,
    dag=dag,
)

# 设置依赖关系
load_reviews_task >> merge_movies_task
load_products_task >> merge_movies_task
merge_movies_task >> normalize_names_task
normalize_names_task >> fill_missing_dates_task
fill_missing_dates_task >> track_lineage_task