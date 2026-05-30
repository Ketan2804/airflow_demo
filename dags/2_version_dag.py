from airflow.sdk import dag, task

@dag(dag_id="version_dag")
def version_dag():
    @task
    def first_task():
        print("This is the first task")
    
    @task
    def second_task():
        print("This is the second task")
    
    @task
    def third_task():
        print("This is the third task")

    @task
    def dag_version():
        print("This is the DAG version task")

    @task
    def fourth_task():  
        print("This is the fourth task")

    first = first_task()
    second = second_task()
    third = third_task()
    fourth = dag_version()
    fifth = fourth_task()

    first >> second >> third >> fourth >> fifth

#instantiate the DAG
version_dag = version_dag()