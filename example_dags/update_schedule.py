from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    'owner': 'owner-name',
    'depends_on_past': False,
    'email': ['your-email@g.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

dag_args = dict(
    dag_id="tutorial-python-op",
    default_args=default_args,
    description='tutorial DAG python',
    schedule_interval=timedelta(minutes=50),
    start_date=datetime(2022, 10,3),
    tags=['example-sj'],
)


def update_schedule():
    import pymysql
    import statsapi
    from DB.DML import DML
    from datetime import date
    dml_instance = DML()
    date.today()
    today = str(date.today())
    today_year = today[0:4]
    today_month = today[5:7]
    today_day = today[8:10]

    date = today_month + "/" + today_day + "/" + today_year
    if int(today_month) != 1:
        last_month = "0" + str((int(today_month) - 1))
        start_date = last_month + "/01/" + today_year
    else:
        start_date = "01/01/" + today_year

    for year in range(int(today_year), int(today_year) - 1, -1):
        games = statsapi.schedule(start_date=start_date, end_date=date)
        for i in games:
            print("===========================")
            print("start update schedules")
            print("===========================")
            game_id = i.get("game_id")
            status = i.get("status")
            home_probable_pitcher = i.get("home_probable_pitcher")
            away_probable_pitcher = i.get("away_probable_pitcher")
            home_pitcher_note = i.get("home_pitcher_note")
            away_pitcher_note = i.get("away_pitcher_note")
            away_score = i.get("away_score")
            home_score = i.get("home_score")
            current_inning = i.get("current_inning")
            inning_state = i.get("inning_state")
            winning_team = i.get("winning_team")
            losing_team = i.get("losing_team")
            winning_pitcher = i.get("winning_pitcher")
            losing_pitcher = i.get("losing_pitcher")
            save_pitcher = i.get("save_pitcher")
            summary = i.get("summary")
            sql = f"update schedules set status = %s, home_probable_pitcher = %s, away_probable_pitcher = %s, home_pitcher_note = %s, away_pitcher_note = %s, away_score = %s, home_score = %s, current_inning = %s,inning_state = %s, winning_team = %s, losing_team = %s, winning_pitcher = %s, losing_pitcher = %s, save_pitcher = %s, summary = %s "
            # sql = f"update schedules set status = '{status}', home_probable_pitcher = '{home_probable_pitcher}', away_probable_pitcher = '{away_probable_pitcher}', home_pitcher_note = '{home_pitcher_note}', away_pitcher_note = '{away_pitcher_note}', away_score = '{away_score}', home_score = '{home_score}', current_inning = '{current_inning}',inning_state = '{inning_state}', winning_team = '{winning_team}', losing_team = '{losing_team}', winning_pitcher = '{winning_pitcher}', losing_pitcher = '{losing_pitcher}', save_pitcher = '{save_pitcher}', summary = '{summary}' "
            sql += f"where game_id = {game_id}"
            vals = (
                status, home_probable_pitcher, away_probable_pitcher, home_pitcher_note, away_pitcher_note, away_score,
                home_score, current_inning, inning_state, winning_team, losing_team, winning_pitcher, losing_pitcher,
                save_pitcher, summary)
            try:
                # dml_instance.execute_sql(sql)
                dml_instance.execute_update_sql(sql, vals)
            except pymysql.err.IntegrityError as e:
                print(e)

    return "update schedule"

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
        bash_command='echo "start!"',
    )

    # update_schedule = PythonOperator(
    #     task_id='update schedule',
    #     python_callable=update_schedule,
    # )

    complete = BashOperator(
        task_id='complete_bash',
        depends_on_past=False,
        bash_command='echo "complete~!"',
        trigger_rule=TriggerRule.NONE_FAILED
    )

    start >> complete
