from DB.DML import DML
from typing import Tuple, Dict
import json
from tqdm import tqdm
from pprint import pprint

# raw_data는 튜플형태
'''
- player_type(batter or pitcher): 이건 맨처음 insert into할 때 투수하고 타자 나눠서 저장해야 함
- player_uid(플레이어 uid): player_uid
- date(날짜 정보): gameData.datetime.officalDate
- game_uid(게임 uid): gameData.game.pk
- season(시즌): gameData.game.season
- weather(날씨정보): gameData.weather.condition
- opponent_uid(상대 플레이어 uid): liveData.plays.allPlays.[index].matchup.pitcher.id, liveData.plays.allPlays.[index].matchup.batter.id
- event_index(이벤트 인덱스): liveData.plays.allPlays.[index] 여기서의 index값
- event_type(이벤트 타입): liveData.plays.allPlays.[index].result.eventType
- player_main플레이어 주 포지션(이건 baseball_players 에 있어서 나중에 join 때려야 할 듯): liveData.plays.allPlays.[index]
- hand(상대 선수의 주손): liveData.plays.allPlays.[index].matchup.batSide.code and liveData.plays.allPlays.[index].matchup.pitchHand.code
- rbi(점수낸 정보): liveData.plays.allPlays.[index].result.rbi
- strikes(현재 스트라이크 카운트): liveData.plays.allPlays.[index].count.strikes
- balls(현재 볼 카운트): liveData.plays.allPlays.[index].count.balls
- outs(현재 아웃카운트): liveData.plays.allPlays.[index].count.outs
- inning(현재 이닝): liveData.plays.allPlays.[index].about.inning
- is_top_inning(현재 초공격인지 말공격인지): liveData.plays.allPlays.[index].about.isTopInning
- at타수 정보인지(at bat): liveData.plays.allPlays.[index].result.type (모든 경우가 atBat 이라 하지 않기)
'''
if __name__ == "__main__":
    dml_instance = DML()
    raw_data: Tuple = None

    game_uids: Tuple = dml_instance.get_select_from_where(column_names=["game_uid"], table_name="game_raw_datas")

    # 1초에 10 iter
    # 게임
    for count, game_uid in tqdm(enumerate(game_uids)):
        condition = f"game_uid={game_uid[0]}"
        raw_data: Tuple = dml_instance.get_select_from_where(column_names=["game_raw_data"],
                                                             table_name="game_raw_datas",
                                                             condition=condition)

        raw_data: dict = json.loads(raw_data[0][0])
        all_plays_len = len(raw_data['liveData']['plays']['allPlays'])

        date = raw_data['gameData']['datetime']['officialDate']
        game_uid = raw_data['gameData']['game']['pk']
        season = raw_data['gameData']['game']['season']
        weather = raw_data['gameData']['weather']['condition']


        batter_sql = "insert into events(player_type, player_uid, date, game_uid, season, weather, opponent_uid, event_index, event_type, player_main_position, opponent_hand, rbi, strikes, balls, outs, inning, is_top_inning) values(%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        pitcher_sql = "insert into events(player_type, player_uid, date, game_uid, season, weather, opponent_uid, event_index, event_type, player_main_position, opponent_hand, rbi, strikes, balls, outs, inning, is_top_inning) values(%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        all_plays = raw_data['liveData']['plays']['allPlays']
        for index in range(all_plays_len):
            batter_id = all_plays[index]['matchup']['batter']['id']
            pitcher_id = all_plays[index]['matchup']['pitcher']['id']
            event_index = index
            event_type = all_plays[index]['result']['eventType']
            player_main_position = ""
            batter_hand = all_plays[index]['matchup']['batSide']['code']
            pitcher_hand = all_plays[index]['matchup']['pitchHand']['code']
            rbi = all_plays[index]['result']['rbi']
            strikes = all_plays[index]['count']['strikes']
            balls = all_plays[index]['count']['balls']
            outs = all_plays[index]['count']['outs']
            inning = all_plays[index]['about']['inning']
            is_top_inning = all_plays[index]['about']['isTopInning']

            batter_vals = ('batters', batter_id, date, game_uid, season, weather, pitcher_id, event_index,
                           event_type, player_main_position, pitcher_hand, rbi, strikes, balls, outs, inning,
                           is_top_inning)

            pitcher_vals = ('pitchers', pitcher_id, date, game_uid, season, weather, batter_id, event_index,
                            event_type, player_main_position, batter_hand, rbi, strikes, balls, outs, inning,
                            is_top_inning)
            dml_instance.execute_insert_sql(batter_sql, batter_vals)
            dml_instance.execute_insert_sql(pitcher_sql, pitcher_vals)
