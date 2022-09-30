import statsapi
from pprint import pprint
from DB.DML import DML

### 어제 진행한 게임

'''
1. 어제 경기기록 불러오는 api (필요한거 = > 어느팀 붙었는지, 점수, MLBTI에서 예측한 결과와 맞는지)
    - request: date  ex(20220101) 숫자 8자리
    - response: home team, home_team_uid, away_team, away_team_uid, home_team_score, away_team_score, is_mlbti_predict_correct
    

2. 오늘 경기일정 불러오는 거( 경기전 → 어느팀 붙을지, MLBTI의 예측 승패 
경기 끝났다면 → 어느팀 붙었는지, 점수, 예측 결과 
3. 각 리그 팀순위 불러오는 api
'''

if __name__ == "__main__":
    # statsapi.schedule(date=None, start_date=None, end_date=None, team="", opponent="", sportId=1, game_id=None)
    yesterday_game_data = statsapi.schedule(date=None, start_date=None, end_date=None, team="", opponent="", sportId=1,
                                            game_id=None)
    ''' 어제 경기 가져오는 로직
    away_id, away_name, away_score, game_date, game_id, home_id, home_name, home_score, current_inning
    1. date를 어제날짜, 그리고 sportId를 1 넣으면 어제 진행한 메이저리그 경기 json 리스트 다 불러옴
    2. 그래서 하루에 한번 데이터를 자동으로 받아오게 해서 db에 저장하고 front에서 하루전 날짜를 넣으면 데이터를 가져오게 하는 로직으로 만들기
    3. 
    away_id int,
    away_name varchar(60),
    game_date date,
    game_id int,
    home_id int,
    home_name varchar(60),
    home_scroe int,
    current_inning int
    '''
    dml_instance = DML()
    vars = []
    _away_id = 0
    _away_name = ""
    _game_date = ""
    _game_id = 0
    _home_id = 0
    _home_name = ""
    _home_score = ""
    _current_inning = 0

    for data in yesterday_game_data:
        _away_id = data["away_id"]
        _away_name = data["away_name"]
        _away_score = data["away_score"]
        _game_date = data["game_date"]
        _game_id = data["game_id"]
        _home_id = data["home_id"]
        _home_name = data["home_name"]
        _home_score = data["home_score"]
        _current_inning = data["current_inning"]
        _status = data["status"]
        vars.append([_away_id, _away_name, _away_score, _game_date, _game_id, _home_id, _home_name, _home_score, _current_inning, _status])
        print(data)

    sql = "insert into games(away_id, away_name, away_score,game_date, game_id, home_id, home_name, home_score, current_inning, status) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    dml_instance.execute_insert_many_sql(sql,vars)


### 오늘 진행할 게임
# https://github.com/toddrob99/MLB-StatsAPI/wiki/Function:-last_game
# statsapi.last_game(teamId)

# 날짜별 경기정보 json 받아오기
# dates = statsapi.get('schedule', {'sportId': 1})['dates']

# pprint(dates)
