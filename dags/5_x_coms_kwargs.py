from airflow.sdk import dag, task

@dag(dag_id="xcoms_dag_kwargs")
def xcoms_dag_kwargs():

    @task
    def first_task(**kwargs):
        ti = kwargs['ti']
        print("Extracting data... This is the first task")

        fetch_data = {"data": [1, 2, 3, 4]}

        ti.xcom_push(key='return_result', value=fetch_data)

    @task
    def second_task(**kwargs):
        ti = kwargs['ti']

        fetched_data = ti.xcom_pull(
            task_ids='first_task',
            key='return_result'
        )

        print("Fetched Data:", fetched_data)

        # Correct transformation
        transform_data = [x * 2 for x in fetched_data["data"]]

        transform_data_dict = {"trans_data": transform_data}

        # Push to XCom
        ti.xcom_push(key='return_result', value=transform_data_dict)

    @task
    def third_task(**kwargs):
        ti = kwargs['ti']

        # Pull from second_task
        final_data = ti.xcom_pull(
            task_ids='second_task',
            key='return_result'
        )

        print("Final Data:", final_data)

    # Dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


# Instantiate DAG
xcoms_dag_kwargs()