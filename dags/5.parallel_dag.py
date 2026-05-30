from airflow.sdk import dag, task
from sqlalchemy import extract

@dag(dag_id="parallel_dag")
def parallel_dag():
    @task
    def extract_task(**kwargs):
        print("Extracting data")
        ti = kwargs['ti']
        extracted_data = {"api_data": [1, 2, 3], "db_data": [4, 5, 6], "file_data": [7, 8, 9]}
        ti.xcom_push(key="returned_value", value=extracted_data)

    @task
    def transform_api_task(**kwargs):
        print("Api data transformation")
        ti = kwargs['ti']
        api_extracted_data =ti.xcom_pull(task_ids="extract_task", key="returned_value")["api_data"]
        transformed_api_data = [i * 10 for i in api_extracted_data]
        ti.xcom_push(key="returned_value", value=transformed_api_data)

    @task
    def db_api_task(**kwargs):
        print("DB data transformation")
        ti = kwargs['ti']
        db_extracted_data =ti.xcom_pull(task_ids="extract_task", key="returned_value")["db_data"]
        transformed_db_data = [i * 100 for i in db_extracted_data]
        ti.xcom_push(key="returned_value", value=transformed_db_data)

    @task
    def file_api_task(**kwargs):
        print("File data transformation")
        ti = kwargs['ti']
        file_extracted_data =ti.xcom_pull(task_ids="extract_task", key="returned_value")["file_data"]
        transformed_file_data = [i * 1000 for i in file_extracted_data]
        ti.xcom_push(key="returned_value", value=transformed_file_data)

    @task.bash
    def load_task(**kwargs):
        print("Loading data")
        ti = kwargs['ti']
        transformed_api_data = ti.xcom_pull(task_ids="transform_api_task", key="returned_value")
        transformed_db_data = ti.xcom_pull(task_ids="db_api_task", key="returned_value")
        transformed_file_data = ti.xcom_pull(task_ids="file_api_task", key="returned_value")
        return f"echo 'data: {transformed_api_data}, {transformed_db_data}, {transformed_file_data}'"



    extract = extract_task()
    transform_api = transform_api_task()
    db_transform = db_api_task()
    file_transform = file_api_task()
    load = load_task()

    extract >> [transform_api, db_transform, file_transform] >> load


#instantiate the DAG
parallel_dag = parallel_dag()