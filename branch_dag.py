from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime 
import random

dag = DAG('branch_test_dag', description="branchtest",
          schedule_interval=None, start_date=datetime(2023,3,5), 
          catchup=False)


def random_numer_generator():
    return random.randint(1,100)

task_rng = PythonOperator(
    task_id='task_rng',
    python_callable = random_numer_generator, dag=dag
)

def avalia_rn(**context):
    number = context ['task_instance'].xcom_pull(task_ids='task_rng')
    if number % 2 == 0:
        return 'par_task'
    else:
        return 'impar_task'


branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=avalia_rn,
    provide_context=True,
    dag=dag
)



par_task = BashOperator(task_id='par_task', bash_command='echo "Numero Par"', dag=dag)
impar_task = BashOperator(task_id='impar_task', bash_command='echo "Numero Impar"', dag=dag)

task_rng >> branch_task
branch_task >> par_task
branch_task >> impar_task

