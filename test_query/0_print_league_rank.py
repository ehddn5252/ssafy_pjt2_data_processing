import statsapi
from pprint import pprint
from DB.DML import DML

### 어제 진행한 게임


if __name__ == "__main__":
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
    print(data)
