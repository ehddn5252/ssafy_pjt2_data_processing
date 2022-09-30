from DB.DML import DML
from typing import Tuple, Dict
import json
from tqdm import tqdm
from pprint import pprint

# raw_data는 튜플형태
# pprint(literal_eval(json.loads(raw_data[0][0])))

if __name__ == "__main__":
    dml_instance = DML()
    raw_data: Tuple = None

    game_uids: Tuple = dml_instance.get_select_from_where(column_names=["game_uid"], table_name="game_raw_datas")
    event_dic: Dict = {}
    event_type_dic: Dict = {}
    pitcher_hand_dic: Dict = {}
    batter_uid_dic: Dict = {}
    pitcher_uid_dic: Dict = {}
    weather_dic: Dict = {}

    # 프로세스
    '''
    0. baseball_players 에서 모든 선수들을 투수, 타자에 나눠서 pitchers, batters 로 저장한다.
        - 여기에서 baseball_players table의 primary_position_type 의 값이 pitcher 가 아니라면 batters로 저장한다..?
        -> 시즌별로 저장을 해야 하므로 맨 처음 초기화 x
    0. 모든 rawdata를 가져온다. 
    1. 한 게임당 gameData.datetime.officalDate 에서 날짜를 가져온다. 여기에서 투수, 타자의 시즌을 구할 수 있다. 
       시즌은 년도 전체(2022 1920 ...)로 한다
    2. live data의 allPlays[i]의 match up 에서 투수 id와 타자 id를 각각 들고온다.
    3. 들고온 투수 id와 년도에 매칭되는 값이 batters에 없다면 batters에 만들고 그 row를 가져온다.
    3.1 만약에 매칭되는 값이 있다면 그 row를 가져온다.
    4. matchup_data['batSide']['code'] 타자면 투수의 손 코드, 투수면 타자의 주손 코드를 가져와 left 에 저장할 지 right 에 저장할 지 정한다.
    5. event 에서 해당하는 값을 database 에 매핑해서 해당하는 값을 하나 저장하고 commit한다.
    '''
    # 1초에 10 iter
    for count, game_uid in tqdm(enumerate(game_uids)):
        condition = f"game_uid={game_uid[0]}"
        raw_data: Tuple = dml_instance.get_select_from_where(column_names=["game_raw_data"],
                                                             table_name="game_raw_datas",
                                                             condition=condition)

        raw_data: dict = json.loads(raw_data[0][0])
        all_plays_len = len(raw_data['liveData']['plays']['allPlays'])
        all_plays = raw_data['liveData']['plays']['allPlays']
        try:
            weather = raw_data['gameData']['weather']['condition']
        except:
            weather = "except"
        # event = raw_data['liveData']['plays']['allPlays'][0]['result']['event']
        if weather not in weather_dic:
            weather_dic[weather] = 1
        else:
            weather_dic[weather] += 1

        for i in range(all_plays_len):
            matchup_data = all_plays[i]['matchup']
            batter_id = matchup_data['batter']['id']
            pitcher_id = matchup_data['pitcher']['id']
            batter_hand = matchup_data['batSide']['code']  # L or R
            pitcher_hand = matchup_data['pitchHand']['code']  # L or R

            try:
                event = all_plays[i]['result']['event']
                event_type = all_plays[i]['result']['eventType']
            except:
                event = "etc"
                event_type = "etc"
            # if pitcher_hand not in hand_dic.keys():

            if event not in event_dic.keys():
                event_dic[event] = 1
            else:
                event_dic[event] += 1

            if event_type not in event_type_dic.keys():
                event_type_dic[event_type] = 1
            else:
                event_type_dic[event_type] += 1
            if event_type=="fielders_choice_out":
                print("==================================================")
                print("필드 초이스 확인")
                pprint(game_uid)
                pprint(i)
                pprint(all_plays[i])
                exit(-1)
        if count == 10000:
            # pprint(event_type_dic)
            # pprint(event_type_dic.keys())
            exit(-1)
