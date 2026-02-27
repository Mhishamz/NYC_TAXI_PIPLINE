from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def hello_world():
    print("Python logic is working inside the container!")

with DAG(
    dag_id='01_test_environment',
    description='A simple DAG to verify Airflow setup',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    # Task 1: Check Python environment
    test_python = PythonOperator(
        task_id='check_python',
        python_callable=hello_world,
    )

    # Task 2: Check System/Bash environment
    test_bash = BashOperator(
        task_id='check_bash',
        bash_command='echo "Bash is working! Current user: $(whoami)"',
    )

    test_python >> test_bash