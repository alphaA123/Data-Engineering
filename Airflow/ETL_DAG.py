from  datetime import date,datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd

with DAG(
    dag_id="sp_500",
    start_date=datetime(2025,11,18),
    schedule_interval=None,
    catchup=False
) as dag:
    task_one=BashOperator(
        task_id="extract_task",
        bash_command='cp -p /workspaces/hands-on-introduction-data-engineering-4395021/data/constituents.csv /workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/sp500-extract-data.csv',
        dag=dag
    )

    def transform_data():
        '''Read in the file, and write a transformed file out'''
        today=date.today()
        df=pd.read_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/sp500-extract-data.csv')
        fil_df=df.groupby('Sector')['Name'].count()
        final_df = fil_df.reset_index().rename(columns={'Name': 'count'})
        final_df['date']=today.strftime('%Y-%m-%d')
        final_df.to_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/sp500-extract-data.csv',index=False)

    task_two=PythonOperator(
            task_id='transform_task',
            python_callable=transform_data,
            dag=dag
        )
    
    sqlite_import_command = """
        echo -e ".separator \",\"\\n.import --skip 1 /workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/sp500-extract-data.csv sp_500_sector_count" | sqlite3 /workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/challenge-load-db.db
    """

    task_three= BashOperator(
        task_id='load_data_to_sqlite',
        bash_command=sqlite_import_command,
        dag=dag,
    )

    task_one >> task_two >> task_three
