from airflow.sdk import dag, task

@dag(dag_id="xcoms_dag_kwargs")
def xcoms_dag_kwargs():
    @task
    def first_task(**kwargs):
        #Extracting ti from kwargs to push data to xcom manually
        ti = kwargs['ti']
        print("Extracting data")
        fetaching_data = {"data": [1, 2, 3, 4, 5]}
        ti.xcom_push(key="return_result", value=fetaching_data)


    @task
    def second_task(**kwargs):
        #Extracting ti from kwargs to pull data from xcom manually
        ti = kwargs['ti']
        print("Transforming data")
        data = ti.xcom_pull(task_ids="first_task", key="return_result")["data"]
        transformed_data = data * 2
        transformed_data_dict = {"transformed_data": transformed_data}
        ti.xcom_push(key="return_result", value=transformed_data_dict)

    @task
    def third_task(**kwargs):
        ti = kwargs['ti']
        loading_data = ti.xcom_pull(task_ids="second_task",  key="return_result")
        return loading_data

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


#instantiate the DAG
xcoms_dag_kwargs = xcoms_dag_kwargs()