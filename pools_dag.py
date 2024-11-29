from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime 

dag = DAG('pool_dag', description="Dag pool",
          schedule_interval=None, start_date=datetime(2023,3,5), 
          catchup=False)

task1 = BashOperator(task_id='tsk1', bash_command="sleep 5", dag=dag, pool="mypool")
task2 = BashOperator(task_id='tsk2', bash_command="sleep 5", dag=dag, pool="mypool", priority_weight=5)
task3 = BashOperator(task_id='tsk3', bash_command="sleep 5", dag=dag, pool="mypool")
task4 = BashOperator(task_id='tsk4', bash_command="sleep 5", dag=dag, pool="mypool", priority_weight=10)