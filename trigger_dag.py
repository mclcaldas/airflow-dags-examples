from airflow import DAG
from airflow.operators.bash_operator import BashOperator 
from airflow.utils.task_group import TaskGroup
from datetime import datetime 

dag = DAG('trigger_dag', description="Trigger Dag",
          schedule_interval=None, start_date=datetime(2023,3,5), 
          catchup=False)

task1 = BashOperator(task_id="tsk1", bash_command= "sleep 5", dag=dag)
task2 = BashOperator(task_id="tsk2", bash_command= "sleep 5", dag=dag)
task3 = BashOperator(task_id="tsk3", bash_command= "sleep 5", dag=dag)
task4 = BashOperator(task_id="tsk4", bash_command= "sleep 5", dag=dag)
task5 = BashOperator(task_id="tsk5", bash_command= "sleep 5", dag=dag)
task6 = BashOperator(task_id="tsk6", bash_command= "sleep 5", dag=dag)
task7 = BashOperator(task_id="tsk7", bash_command= "exit 1", dag=dag)
task8 = BashOperator(task_id="tsk8", bash_command= "sleep 5", dag=dag)
task9 = BashOperator(task_id="tsk9", bash_command= "sleep 5", dag=dag, 
                     trigger_rule='all_failed')
task10 = BashOperator(task_id="tsk10", bash_command= "sleep 5", dag=dag, 
                     trigger_rule='one_failed')


tsk_group = TaskGroup("tsk_grop", dag=dag)

task_grp1 = BashOperator(task_id="task_grp1", bash_command= "sleep 5", dag=dag, task_group=tsk_group)
task_grp2 = BashOperator(task_id="task_grp2", bash_command= "sleep 5", dag=dag, task_group=tsk_group)

task1 >> task2
task3 >> task4
[task2,task4] >> task5 >> task6
task6 >> [task7,task8,task9]
[task7,task8,task9] >> task10
task10 >> [task_grp1, task_grp2]