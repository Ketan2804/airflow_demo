from airflow.sdk import dag, task

@dag(dag_id="first_dag")
def first_dag():
    @task
    def first_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")

    @task
    def third_task():
        print("This is the third task")

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


#instantiate the DAG
first_dag = first_dag()