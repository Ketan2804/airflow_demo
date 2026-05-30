from airflow.sdk import dag, task

@dag(dag_id="xcoms_dag_auto")
def xcoms_dag_auto():
    @task
    def first_task():
        print("Extracting data")
        fetaching_data = {"data": [1, 2, 3, 4, 5]}
        return fetaching_data

    @task
    def second_task(fetaching_data):
        print("Transforming data")
        data = fetaching_data["data"]
        transformed_data = data * 2
        transformed_data_dict = {"transformed_data": transformed_data}
        return transformed_data_dict

    @task
    def third_task(data):
        loading_data = data
        return loading_data

    first = first_task()
    second = second_task(first)
    third = third_task(second)

    first >> second >> third


#instantiate the DAG
xcoms_dag_auto = xcoms_dag_auto()