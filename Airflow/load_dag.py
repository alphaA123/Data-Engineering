'''Load DAG'''
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow import DAG

with DAG(
    dag_id='load_dag',
    start_date=datetime(2015,11,18),
    schedule_interval=None,
    catchup=False
) as dag:
    
    load_task=BashOperator(
        task_id='load_task',
        bash_command='echo -e ".seperator "," \n.import --skip 1 /workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/airflow-transform-data.csv top_level_domains" | sqlite3 /workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/airflow-load-db.db',
        dag=dag
    )