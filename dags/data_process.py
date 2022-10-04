from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import sys,os
sys.path.append("/home/ubuntu/airflow/ssafy_pjt2_data_processing")

from DB.DML import DML

default_args = {
    'owner': 'owner-name',
    'depends_on_past': False,
    'email': ['ehddn5252@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

dag_args = dict(
    dag_id="mlb-data-updater",
    default_args=default_args,
    description='update mlbti',
    schedule_interval = timedelta(minutes=2400),
    start_date=datetime(2022,10,4),
    tags=['example-sj'],
)

def print_date():
    from datetime import date
    print("test is running")
    print(datetime.now())

def branch_path():
    import statsapi
    data = statsapi.standings_data(leagueId="103,104", division="all", include_wildcard=True, season=None,standingsTypes=None, date=None)
    print(data)

def update_scheduler2():
    pass



with DAG(**dag_args) as dag:
    start = BashOperator(
        task_id='start',
        bash_command='echo "start"',
    )

    update_schedule = PythonOperator(
        task_id='update',
        python_callable=update_scheduler2,
    )

    now_date = PythonOperator (
        task_id = 'print_today',
        python_callable=print_date,
    )

    complete = BashOperator(
        task_id='complete_bash',
        depends_on_past=False,
        bash_command='echo "complete~!"',
        trigger_rule=TriggerRule.NONE_FAILED
    )

    start >> now_date >> update_schedule >> complete
   #  start >> update_schedule >> complete
