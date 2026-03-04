from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="hello_world_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # trigger manually
    catchup=False,
    tags=["example"],
)
def hello_world():
      @task
      def print_message():
          print("Hello from Airflow DAG!")

      print_message()


hello_world()