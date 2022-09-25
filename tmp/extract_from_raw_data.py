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

    # 프로세스
    '''
    1. live data의 allPlays[i]의 match up 에서 선수 id를 가져온다.
    2. 
    '''
    # 1초에 10 iter
    for count, game_uid in tqdm(enumerate(game_uids)):
        condition = f"game_uid={game_uid[0]}"
        raw_data: Tuple = dml_instance.get_select_from_where(column_names=["game_raw_data"],
                                                             table_name="game_raw_datas",
                                                             condition=condition)

        raw_data: dict = json.loads(raw_data[0][0])
        # print("===================================")
        # print("raw_data['liveData'].keys()")
        # print(raw_data['liveData'].keys())
        #
        # print(raw_data['liveData']['linescore'].keys())
        # # all plays

        all_plays_len = len(raw_data['liveData']['plays']['allPlays'])
        all_plays = raw_data['liveData']['plays']['allPlays']

        # event = raw_data['liveData']['plays']['allPlays'][0]['result']['event']

        for i in range(all_plays_len):
            '''
            matchup_data = all_plays[i]['matchup']
            batter_id = matchup_data['batter']['id']
            pitcher_id = matchup_data['pitcher']['id']
            batter_hand = matchup_data['batSide']['code']  # L or R
            pitcher_hand = matchup_data['pitchHand']['code']  # L or R
            '''
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

        if count == 50000:
            pprint(event_type_dic)
            pprint(event_type_dic.keys())
            exit(-1)
