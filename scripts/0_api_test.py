import statsapi
from pprint import pprint
from DB.DML import DML
from typing import List
from tqdm import tqdm

if __name__ == "__main__":
    # print(statsapi.player_stats(next(
    #     x['id'] for x in statsapi.get('sports_players', {'season': 2008, 'gameType': 'W'})['people'] if
    #     x['fullName'] == 'Chase Utley'), 'hitting', 'career'))
    # for player in statsapi.lookup_player('Harper'):
    #    print(player)
    # https://statsapi.mlb.com/api/v1.1/standings
    #
    # https://statsapi.mlb.com/api/{ver}/sports/{sportId}/players

    print("hi")
    statsapi.roster(teamId, rosterType=None, season=datetime.now().year, date=None)