import statsapi
from pprint import pprint
from DB.DML import DML
from datetime import date
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
    dml_instance = DML()
    vars=[]
    _league_code = 0
    _date = str(date.today()).replace("-","")
    _div_name = ""
    _div_rank = ""
    _elim_num=""
    _gb = ""
    _l = 0
    _league_rank = 0
    _name = ""
    _sport_rank = ""
    _team_id = 0
    _w = 0
    _wc_elim_num = ""
    _wc_gb = ""
    _wc_rank = ""
    for key, value in data.items():
        print(value)
        _league_code = key
        _div_name = value['div_name']
        for i,v in enumerate(value['teams']):
            _name = v['name']
            _div_rank = v['div_rank']
            _w= v['w']
            _l = v['l']
            _gb = v['gb']
            _wc_rank = v['wc_rank']
            _wc_gb = v['wc_gb']
            _wc_elim_num = v['wc_elim_num']
            _elim_num= v['elim_num']
            _team_id = v['team_id']
            _league_rank = v['league_rank']
            _sport_rank = v['sport_rank']
            vars.append([_league_code,_date,_div_name,_div_rank,_elim_num,_gb,_l,_league_rank,_name,_sport_rank,_team_id,_w,_wc_elim_num,_wc_gb,_wc_rank])
    sql = "insert into league_rank(league_code,date,div_name,div_rank,elim_num,gb,l,league_rank,name,sport_rank,team_id,w,wc_elim_num,wc_gb,wc_rank) values(%s ,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    dml_instance.execute_insert_many_sql(sql,vars)
