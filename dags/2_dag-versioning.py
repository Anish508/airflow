from airflow.sdk import dag, task

@dag(dag_id="versioned_dag")
def versioned_dag():

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


    # defining the task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    virsion = versioned_task()

    first >> second >> third >> virsion


# instantiating the dag
versioned_dag()