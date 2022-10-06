from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import sys,os
sys.path.append(os.getcwd())
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
    dag_id="update-scheduler",
    default_args=default_args,
    description='update mlbti',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2022,10,4),
    tags=['example-sj'],
)

def print_date():
    from datetime import date
    from random import randint
    print("test is running")
    print(datetime.now())
    #print(date.today())

def branch_path():
    import statsapi
    data = statsapi.standings_data(leagueId="103,104", division="all", include_wildcard=True, season=None,standingsTypes=None, date=None)
    print(data)

def update_rank():

    import statsapi
    from datetime import date
    '''
        https://github.com/toddrob99/MLB-StatsAPI/wiki
        200 아메리카 서부 리그
        201 아메리카 동부 리그
        202 아메리카 중부 리그
        203 네셔널 서부 리그
        204 네셔널 동부 리그
        205 네셔널 중부 리그
    '''
    data = statsapi.standings_data(leagueId="103,104", division="all", include_wildcard=True, season=None,
                                   standingsTypes=None, date=None)
    dml_instance = DML()
    vars = []
    _league_code = 0
    _date = str(date.today()).replace("-", "")
    _div_name = ""
    _div_rank = ""
    _elim_num = ""
    _gb = ""
    _l = 0
    _league_rank = 0
    _name = ""
    _sport_rank = ""
    _team_id = 0
    _w = 0
    _wc_elim_num = ""
    _wc_gb = ""
    _wc_rank = ""
    for key, value in data.items():
        print(value)
        _league_code = key
        _div_name = value['div_name']
        for i, v in enumerate(value['teams']):
            _name = v['name']
            _div_rank = v['div_rank']
            _w = v['w']
            _l = v['l']
            _gb = v['gb']
            _wc_rank = v['wc_rank']
            _wc_gb = v['wc_gb']
            _wc_elim_num = v['wc_elim_num']
            _elim_num = v['elim_num']
            _team_id = v['team_id']
            _league_rank = v['league_rank']
            _sport_rank = v['sport_rank']
            vars.append(
                [_league_code, _date, _div_name, _div_rank, _elim_num, _gb, _l, _league_rank, _name, _sport_rank,
                 _team_id, _w, _wc_elim_num, _wc_gb, _wc_rank])
    sql = "insert into league_rank(league_code,date,div_name,div_rank,elim_num,gb,l,league_rank,name,sport_rank,team_id,w,wc_elim_num,wc_gb,wc_rank) values(%s ,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    dml_instance.execute_insert_many_sql(sql, vars)



def print_result(**kwargs):
    r = kwargs["task_instance"].xcom_pull(key='calc_result')
    print("message : ", r)
    print("*" * 100)
    print(kwargs)


def end_seq():
    print("end")


with DAG(**dag_args) as dag:
    start = BashOperator(
        task_id='start',
        bash_command='echo "start"',
    )

    update_schedule = PythonOperator(
        task_id='update',
        python_callable=update_rank,
    )

    now_date = PythonOperator (
        task_id = 'print_today',
        python_callable=print_date,
    )
    # branch = PythonOperator(
    #     task_id = 'branch_test',
    #     python_callable=branch_path
    # )

    complete = BashOperator(
        task_id='complete_bash',
        depends_on_past=False,
        bash_command='echo "complete~!"',
        trigger_rule=TriggerRule.NONE_FAILED
    )

    start >> now_date >> update_schedule >> complete
   #  start >> update_schedule >> complete
