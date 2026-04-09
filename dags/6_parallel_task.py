from airflow.sdk import dag, task

@dag(dag_id="parallel_dag")
def parallel_dag():

    @task
    def extract_task(**kwargs):
        ti = kwargs["ti"]
        print("Extracting the data...")

        extract_data_dict = {
            'api_data': [1,2,3,4],
            'db_extracted_data': [5,6,7,8],
            's3_extracted_data': [9,10,11,12]
        }

        ti.xcom_push(key='extract_data', value=extract_data_dict)

    @task
    def transform_task_api(**kwargs):
        ti = kwargs['ti']

        data = ti.xcom_pull(task_ids='extract_task', key='extract_data')
        transformed = [x*10 for x in data['api_data']]

        ti.xcom_push(key='api_data', value=transformed)

    @task
    def transform_task_db(**kwargs):
        ti = kwargs['ti']

        data = ti.xcom_pull(task_ids='extract_task', key='extract_data')
        transformed = [x*10 for x in data['db_extracted_data']]

        ti.xcom_push(key='db_data', value=transformed)

    @task
    def transform_task_s3(**kwargs):
        ti = kwargs['ti']

        data = ti.xcom_pull(task_ids='extract_task', key='extract_data')
        transformed = [x*10 for x in data['s3_extracted_data']]

        ti.xcom_push(key='s3_data', value=transformed)

    @task
    def load_data(**kwargs):
        ti = kwargs['ti']
        print("Loading data to destination...")

        api_data = ti.xcom_pull(task_ids="transform_task_api", key="api_data")
        db_data = ti.xcom_pull(task_ids="transform_task_db", key="db_data")
        s3_data = ti.xcom_pull(task_ids="transform_task_s3", key="s3_data")

        print("Loaded:", api_data, db_data, s3_data)

    # DAG flow
    extract = extract_task()

    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()

    load = load_data()

    extract >> [transform_api, transform_db, transform_s3] >> load


parallel_dag()