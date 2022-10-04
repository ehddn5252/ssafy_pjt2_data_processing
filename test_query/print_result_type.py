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
    event_count=0
    game_uids: Tuple = dml_instance.get_select_from_where(column_names=["game_uid"], table_name="game_raw_datas")
    result_type_dic: Dict = {}
    # 1초에 10 iter
    for count, game_uid in tqdm(enumerate(game_uids)):
        condition = f"game_uid={game_uid[0]}"
        raw_data: Tuple = dml_instance.get_select_from_where(column_names=["game_raw_data"],
                                                             table_name="game_raw_datas",
                                                             condition=condition)

        raw_data: dict = json.loads(raw_data[0][0])
        all_plays_len = len(raw_data['liveData']['plays']['allPlays'])
        all_plays = raw_data['liveData']['plays']['allPlays']


        for i in range(all_plays_len):
            event_count+=1
            try:
                type = all_plays[i]['result']['type']
            except:
                type = "etc"
            # if pitcher_hand not in hand_dic.keys():
            if type!="atBat":
                print(type)
            if type not in result_type_dic.keys():
                result_type_dic[type] = 1
            else:
                result_type_dic[type] += 1

        if count == 10:
            pprint("")
            pprint("event_count")
            pprint(event_count)
            pprint(result_type_dic)
            exit(-1)
