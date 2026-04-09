from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(
    dag_id="operators_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)
def operators_dag():

    @task
    def first_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")

    @task
    def third_task():
        print("This is the third task. Dag completed!")

    @task
    def versioned_task():
        print("This is the fourth task. dag version 2.0!")

    # Bash using decorator
    @task.bash
    def run_after_loop():
        return "echo https://airflow.apache.org/"

    # Bash using operator
    also_run_this = BashOperator(
        task_id="also_run_this",
        bash_command='echo "ti_key={{ task_instance_key_str }}"',
    )

    # Task execution
    first = first_task()
    second = second_task()
    third = third_task()
    version = versioned_task()
    bash_task = run_after_loop()

    # Dependencies
    first >> second >> third >> version >> bash_task >> also_run_this


# Instantiate DAG
operators_dag()