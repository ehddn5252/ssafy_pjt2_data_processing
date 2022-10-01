import statsapi
from pprint import pprint
from DB.DML import DML
from typing import List
from tqdm import tqdm

if __name__ == "__main__":

    dml_instance = DML()
    '''
    https://github.com/toddrob99/MLB-StatsAPI/wiki
    standing data
    1. pitchers에서 player_uid 가져옴
    2. player_uid 가 baseball_players 에서 같은 친구의 primary_position_abbreviation 을 가져옴
    '''
    sql=f"update pitchers set primary_position_abbreviation = 'p' where primary_position_abbreviation is null"
    dml_instance.execute_update_sql(sql)


