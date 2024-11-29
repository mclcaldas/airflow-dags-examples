from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime 



dag = DAG('database_dag', 
          description="Database",
          schedule_interval=None, 
          start_date=datetime(2023,3,5), 
          catchup=False)

def print_result(ti):
    task_instance = ti.xcom_pull(task_ids='query_table')
    print("Resultado da consulta:")
    for row in task_instance:
        print(row)


create_table = PostgresOperator(task_id='create_table', 
                                postgres_conn_id='postgres',
                                sql='create table if not exists teste(id int);',
                                dag=dag)

insert_table = PostgresOperator(task_id='insert_table', 
                                postgres_conn_id='postgres',
                                sql='insert into teste values(1);',
                                dag=dag)

query_table = PostgresOperator(task_id='query_table', 
                                postgres_conn_id='postgres',
                                sql='select * from teste;',
                                dag=dag)


print_result_task = PythonOperator(task_id='print_result_task',
                                   python_callable=print_result,
                                   provide_context=True,
                                   dag=dag)

create_table >> insert_table >> query_table >> print_result_task
