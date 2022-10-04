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
    schedule_interval = timedelta(days=3),
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

def insert_scheduler():
    dml_instance = DML()
    sql = f"select distinct(game_id) from new_schedules;"
    dml_instance.execute(sql, [])
    schedule_game_ids = dml_instance.fetch_all()
    game_id_list = []
    for i in schedule_game_ids:
        game_id_list.append(i[0])
    for year in range(2022, 2000, -1):
        print('year: ' + str(year))
        games = statsapi.schedule(start_date='01/01/' + str(year), end_date='12/31/' + str(year))
        print(len(games))
        for i in games:
            if i.get("game_id") in game_id_list:
                print("it is in game_id_list")
                continue

            print(i.get("game_id"))
            game_id = i.get("game_id")
            game_datetime = i.get("game_datetime")
            game_date = i.get("game_date")
            game_type = i.get("game_type")
            status = i.get("status")
            away_name = i.get("away_name")
            home_name = i.get("home_name")
            away_id = i.get("away_id")
            home_id = i.get("home_id")
            doubleheader = i.get("doubleheader")
            game_num = i.get("game_num")
            home_probable_pitcher = i.get("home_probable_pitcher")
            away_probable_pitcher = i.get("away_probable_pitcher")
            home_pitcher_note = i.get("home_pitcher_note")
            away_pitcher_note = i.get("away_pitcher_note")
            away_score = i.get("away_score")
            home_score = i.get("home_score")
            current_inning = i.get("current_inning")
            inning_state = i.get("inning_state")
            venue_id = i.get("venue_id")
            venue_name = i.get("venue_name")
            winning_team = i.get("winning_team")
            losing_team = i.get("losing_team")
            winning_pitcher = i.get("winning_pitcher")
            losing_pitcher = i.get("losing_pitcher")
            save_pitcher = i.get("save_pitcher")
            summary = i.get("summary")
            sql = "insert into new_schedules(game_id, game_datetime, game_date, game_type, status, away_name, home_name, away_id, home_id, doubleheader, game_num, home_probable_pitcher, away_probable_pitcher, home_pitcher_note, away_pitcher_note, away_score, home_score, current_inning, inning_state, venue_id, venue_name, winning_team, losing_team,winning_pitcher, losing_pitcher, save_pitcher, summary) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            vals = (
            game_id, game_datetime, game_date, game_type, status, away_name, home_name, away_id, home_id, doubleheader,
            game_num, home_probable_pitcher, away_probable_pitcher, home_pitcher_note, away_pitcher_note, away_score,
            home_score, current_inning, inning_state, venue_id, venue_name, winning_team, losing_team, winning_pitcher,
            losing_pitcher, save_pitcher, summary)
            try:
                dml_instance.execute(sql, vals)
            except pymysql.err.IntegrityError as e:
                print(e)
            dml_instance.commit()



with DAG(**dag_args) as dag:
    start = BashOperator(
        task_id='start',
        bash_command='echo "start"',
    )

    update_schedule = PythonOperator(
        task_id='update',
        python_callable=insert_scheduler,
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
