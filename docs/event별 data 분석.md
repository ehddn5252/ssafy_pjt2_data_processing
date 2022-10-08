Pitchers 데이터 쌓는 쿼리

1차 정제
game_raw_datas(100GB) 21만건 -> new_events 12시간 (2300만건)

## game_raw_datas table
![image](https://user-images.githubusercontent.com/51036842/192943753-deb53224-8fc3-4ff5-a100-b013b1f907d6.png)

```sql
  `uid` int NOT NULL AUTO_INCREMENT,
  `game_uid` int unsigned DEFAULT NULL,
  `game_raw_data` json DEFAULT NULL
```

## new_events table
![image](https://user-images.githubusercontent.com/51036842/192943897-71171250-7f3a-4045-b185-46fe7b85a9e5.png)

![image](https://user-images.githubusercontent.com/51036842/192943980-48bd8fd2-2747-4201-a33f-5d8ae2338075.png)

```sql
  `uid` int NOT NULL AUTO_INCREMENT,
  `player_type` char(20) DEFAULT NULL,
  `player_uid` int DEFAULT NULL,
  `date` varchar(20) DEFAULT NULL,
  `game_uid` int unsigned DEFAULT NULL,
  `season` char(10) DEFAULT NULL,
  `weather` varchar(20) DEFAULT NULL,
  `opponent_uid` int unsigned DEFAULT NULL,
  `event_index` smallint unsigned DEFAULT NULL,
  `event` varchar(60) DEFAULT NULL,
  `event_type` varchar(60) DEFAULT NULL,
  `player_main_position` varchar(20) DEFAULT NULL,
  `opponent_hand` varchar(6) DEFAULT NULL,
  `rbi` tinyint unsigned DEFAULT NULL,
  `strikes` tinyint unsigned DEFAULT NULL,
  `balls` tinyint unsigned DEFAULT NULL,
  `outs` tinyint unsigned DEFAULT NULL,
  `inning` tinyint unsigned DEFAULT NULL,
  `is_top_inning` tinyint(1) DEFAULT NULL,
  `type` varchar(20) DEFAULT NULL,
  `creater` varchar(20) DEFAULT NULL,
  `updater` varchar(20) DEFAULT NULL,
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
```

```sql

select uid, player_uid, season, 



select season, player_uid, opponent_hand, sum(rbi), sum(strikes), sum(balls), sum(outs) 
from new_events 
group by season, player_uid, opponent_hand;
```

## event_player 
![image](https://user-images.githubusercontent.com/51036842/192943286-a1562940-2232-42b6-ba06-6d6dd3e07177.png)

```sql
CREATE TABLE `event_player` (
  `uid` int NOT NULL AUTO_INCREMENT,
  `player_uid` int DEFAULT NULL,
  `season` char(10) DEFAULT NULL,
  `player_type` char(20) DEFAULT NULL,
  `opponent_hand` varchar(6) DEFAULT NULL,
  `event_type` varchar(60) DEFAULT NULL,
  `count` int DEFAULT NULL,
  PRIMARY KEY (`uid`),
  KEY `player_uid` (`player_uid`,`season`,`event_type`,`opponent_hand`)
)
```

## event_pitcher_counts
![image](https://user-images.githubusercontent.com/51036842/192944358-9a696fbc-1f17-4155-b714-4ee378ecd03a.png)
```sql
  `uid` int NOT NULL AUTO_INCREMENT,
  `player_uid` int DEFAULT NULL,
  `season` char(10) DEFAULT NULL,
  `opponent_hand` varchar(6) DEFAULT NULL,
  `rbi` int DEFAULT NULL,
  `strike` int DEFAULT NULL,
  `ball` int DEFAULT NULL,
  `count` int DEFAULT NULL,
```
```sql
--  event_pitcher_counts
insert into event_pitcher_counts( player_uid, season, opponent_hand, rbi, strike, ball, count)
select player_uid, season, opponent_hand, sum(rbi), sum(strikes), sum(balls), sum(balls)+sum(strikes)
from new_events
where player_type="pitchers"
group by season, player_uid, opponent_hand;
```


## new_event_player
![image](https://user-images.githubusercontent.com/51036842/192944795-737f942d-b473-43d9-8acf-a8f16f190908.png)
```sql
  `uid` int NOT NULL AUTO_INCREMENT,
  `player_uid` int DEFAULT NULL,
  `season` char(10) DEFAULT NULL,
  `player_type` char(20) DEFAULT NULL,
  `opponent_hand` varchar(6) DEFAULT NULL,
  `event` varchar(60) DEFAULT NULL,
  `count` int DEFAULT NULL,
```

event_player 만드는 로직
```sql
insert into new_event_player(player_uid, season, player_type, opponent_hand, event, count)
select player_uid,season,player_type, opponent_hand,event, count(uid)
from new_events
group by player_uid, season, player_type, opponent_hand, event;
```

event_pitcher 만드는 로직

일단 event_type을 분석해서 is_hit, at_bat, pa 를 분석해야 한다.
```sql
# uid, event, is_hit, at_bat, pa
1, Walk,0,0,1,
2, Single,1,1,1
3, Forceout,0,1,1
4, Groundout,0,1,1
5, Flyout,0,1,1
6, Strikeout,0,1,1
7, Double,1,1,1
8, Lineout,0,1,1
9, Sac Bunt,0,0,1
10, Bunt Pop Out,0,1,1
11, Home Run,1,1,1
12, Pop Out,0,1,1
13, Field Error,0,0,0
14, Hit By Pitch,1,1,1
15, Grounded Into DP,0,1,1
16, Caught Stealing 2B,0,0,0
17, Triple,1,1,1
18, Intent Walk,0,0,1
19, Fielders Choice Out,0,0,1
20, Bunt Groundout,0,1,1
21, Double Play,0,1,1
22, Sac Fly,0,0,1
23, Pickoff Caught Stealing 2B,0,0,0
24, Strikeout Double Play,0,1,1
25, Caught Stealing Home,0,0,0
26, Fielders Choice,0,0,0
27, Runner Out,0,0,0
28, Pickoff 1B,0,0,0
29, Sac Fly Double Play,0,1,1
30, Batter Out,0,1,1
31, Caught Stealing 3B,0,0,0
32, Wild Pitch,0,0,0
33, Pickoff Caught Stealing 3B,0,0,0
34, Pickoff 2B,0,0,0
35, Pickoff 3B,0,0,0
36, Pickoff Caught Stealing Home,0,0,0   
37, Catcher Interference,0,0,1
38, Pitching Substitution,0,0,0
39, Stolen Base 2B,0,0,0
40, Triple Play,0,1,1
41, Defensive Switch,0,0,0
42, Balk,0,0,0
43, Defensive Sub,0,0,0
44, Runner Double Play,0,0,0
45, Sac Bunt Double Play,0,1,1
46, Pickoff Error 3B,0,0,0
47, etc
48, Passed Ball,0,0,0
49, Strikeout Triple Play,0,1,1
50, Bunt Lineout,0,1,1
51, Pickoff Error 2B,0,0,0
52, Stolen Base 3B,0,0,0
53, Pickoff Error 1B,0,0,0
54, Field Out,0,1,1
55, Defensive Indiff,0,0,0
56, Injury,0,0,0
57, Other Advance,0,0,0
58, Stolen Base Home,0,0,0
59, Game Advisory,0,0,0
60, Error,0,0,0
61, Ejection,0,0,0
62, Offensive Substitution,0,0,0
63, Cs Double Play,0,1,1

```
37번 포수간섭은 어디에 속함?
일단 0,0,0 으로 설정

event_player 에 따라 적절한 곳으로 

event_pitcher 작성 쿼리 
```sql
insert into event_pitcher(player_uid,season,opponent_hand,event,event_type,count, is_hit, at_bat, pa) 
select _player_uid, _season, _opponent_hand, _event, _event_type, _count, _is_hit, _at_bat, _pa
from event_player
where player_type="pitchers"
```

```sql
insert into event_pitcher(player_uid, season, opponent_hand, event, count)
select player_uid,season, opponent_hand,event, count(uid)
from new_events
where player_type="pitchers"
group by player_uid, season, opponent_hand, event;
```

```sql

update event_batters
set at_bat = true
where event_type = "double" or event_type = "single" or event_type = "triple" or event_type = "home_run" ...
```


event_pitcher 로직

```sql
drop procedure if exists test1;

DELIMITER $$
create procedure TEST1(_player_uid INT, _season INT)
BEGIN
	
    declare _opponent_hand varchar(10);
    declare _count int;
	  declare _is_hit bit(1);
    declare _at_bat bit(1);
    declare _pa bit(1);
    declare _event varchar(60);

select 
  case
    when (event="Walk") then 0
    when (event="Single") then 1
    when (event="Forceout") then 0
    when (event="Groundout") then 0
    when (event="Flyout") then 0
    when (event="Strikeout") then 0
    when (event="Double") then 1
    when (event="Lineout") then 0
    when (event="Sac Bunt") then 0
    when (event="Bunt Pop Out") then 0
    when (event="Home Run") then 0
    when (event="Pop Out") then 0
    when (event="Field Error") then 0
    when (event="Hit By Pitch") then 1
    when (event="Grounded Into DP") then 0
    when (event="Caught Stealing 2B") then 0
    when (event="Triple") then 0
    when (event="Intent Walk") then 0
    when (event="Fielders Choice Out") then 0
    when (event="Bunt Groundout") then 0
    when (event="Double Play") then 0
    when (event="Sac Fly") then 0
    when (event="Pickoff Caught Stealing 2B") then 0
    when (event="Strikeout Double Play") then 0
    when (event="Caught Stealing Home") then 0
    when (event="Fielders Choice") then 0
    when (event="Runner Out") then 0
    when (event="Pickoff 1B") then 0
    when (event="Sac Fly Double Play,") then 0
    when (event="Batter Out") then 0
    when (event="Caught Stealing 3B") then 0
    when (event="Wild Pitch") then 0
    when (event="Pickoff Caught Stealing 3B") then 0
    when (event="Pickoff 2B") then 0
    when (event="Pickoff 3B") then 0
    when (event="Pickoff Caught Stealing Home") then 0
    when (event="Catcher Interference") then 0
    when (event="Pitching Substitution") then 0
    when (event="Stolen Base 2B") then 0
    when (event="Triple Play") then 0
    when (event="Defensive Switch") then 0
    when (event="Balk") then 0
    when (event="Defensive Sub") then 0
    when (event="Runner Double Play") then 0
    when (event="Sac Bunt Double Play") then 0
    when (event="Pickoff Error 3B") then 0
    when (event="etc") then 0
    when (event="Passed Ball") then 0
    when (event="Strikeout Triple Play") then 0
    when (event="Bunt Lineout") then 0
    when (event="Pickoff Error 2B") then 0
    when (event="Stolen Base 3B") then 0
    when (event="Pickoff Error 1B") then 0
    when (event="Field Out") then 0
    when (event="Defensive Indiff") then 0
    when (event="Injury") then 0
    when (event="Other Advance") then 0
    when (event="Stolen Base Home") then 0
    when (event="Game Advisory") then 0
    when (event="Error") then 0
    when (event="Ejection") then 0
    when (event="Offensive Substitution") then 0
    when (event="Cs Double Play") then 0
    else 1
  end
into _is_hit
from new_event_player
where player_type="pitcher";

select 
  case
    when (event="Walk") then 0
    when (event="Single") then 1
    when (event="Forceout") then 1
    when (event="Groundout") then 1
    when (event="Flyout") then 1
    when (event="Strikeout") then 1
    when (event="Double") then 1
    when (event="Lineout") then 1
    when (event="Sac Bunt") then 0
    when (event="Bunt Pop Out") then 1
    when (event="Home Run") then 1
    when (event="Pop Out") then 1
    when (event="Field Error") then 0
    when (event="Hit By Pitch") then 1
    when (event="Grounded Into DP") then 1
    when (event="Caught Stealing 2B") then 0
    when (event="Triple") then 1
    when (event="Intent Walk") then 0
    when (event="Fielders Choice Out") then 0
    when (event="Bunt Groundout") then 1
    when (event="Double Play") then 1
    when (event="Sac Fly") then 0
    when (event="Pickoff Caught Stealing 2B") then 0
    when (event="Strikeout Double Play") then 1
    when (event="Caught Stealing Home") then 0
    when (event="Fielders Choice") then 0
    when (event="Runner Out") then 0
    when (event="Pickoff 1B") then 0
    when (event="Sac Fly Double Play,") then 1
    when (event="Batter Out") then 1
    when (event="Caught Stealing 3B") then 0
    when (event="Wild Pitch") then 0
    when (event="Pickoff Caught Stealing 3B") then 0
    when (event="Pickoff 2B") then 0
    when (event="Pickoff 3B") then 0
    when (event="Pickoff Caught Stealing Home") then 0
    when (event="Catcher Interference") then 0
    when (event="Pitching Substitution") then 0
    when (event="Stolen Base 2B") then 0
    when (event="Triple Play") then 1
    when (event="Defensive Switch") then 0
    when (event="Balk") then 0
    when (event="Defensive Sub") then 0
    when (event="Runner Double Play") then 0
    when (event="Sac Bunt Double Play") then 1
    when (event="Pickoff Error 3B") then 0
    when (event="etc") then 0
    when (event="Passed Ball") then 0
    when (event="Strikeout Triple Play") then 1
    when (event="Bunt Lineout") then 1
    when (event="Pickoff Error 2B") then 0
    when (event="Stolen Base 3B") then 1
    when (event="Pickoff Error 1B") then 0
    when (event="Field Out") then 1
    when (event="Defensive Indiff") then 0
    when (event="Injury") then 0
    when (event="Other Advance") then 0
    when (event="Stolen Base Home") then 0
    when (event="Game Advisory") then 0
    when (event="Error") then 0
    when (event="Ejection") then 0
    when (event="Offensive Substitution") then 0
    when (event="Cs Double Play") then 1
    else 1
  end
into _at_bat
from new_event_player
where player_type="pitcher";

select 
  case
    when (event="Walk") then 1
    when (event="Single") then 1
    when (event="Forceout") then 1
    when (event="Groundout") then 1
    when (event="Flyout") then 1
    when (event="Strikeout") then 1
    when (event="Double") then 1
    when (event="Lineout") then 1
    when (event="Sac Bunt") then 1
    when (event="Bunt Pop Out") then 1
    when (event="Home Run") then 1
    when (event="Pop Out") then 1
    when (event="Field Error") then 0
    when (event="Hit By Pitch") then 1
    when (event="Grounded Into DP") then 1
    when (event="Caught Stealing 2B") then 0
    when (event="Triple") then 1
    when (event="Intent Walk") then 1
    when (event="Fielders Choice Out") then 1
    when (event="Bunt Groundout") then 1
    when (event="Double Play") then 1
    when (event="Sac Fly") then 1
    when (event="Pickoff Caught Stealing 2B") then 0
    when (event="Strikeout Double Play") then 1
    when (event="Caught Stealing Home") then 0
    when (event="Fielders Choice") then 0
    when (event="Runner Out") then 0
    when (event="Pickoff 1B") then 0
    when (event="Sac Fly Double Play,") then 1
    when (event="Batter Out") then 1
    when (event="Caught Stealing 3B") then 0
    when (event="Wild Pitch") then 0
    when (event="Pickoff Caught Stealing 3B") then 0
    when (event="Pickoff 2B") then 0
    when (event="Pickoff 3B") then 0
    when (event="Pickoff Caught Stealing Home") then 0
    when (event="Catcher Interference") then 1
    when (event="Pitching Substitution") then 0
    when (event="Stolen Base 2B") then 0
    when (event="Triple Play") then 1
    when (event="Defensive Switch") then 0
    when (event="Balk") then 0
    when (event="Defensive Sub") then 0
    when (event="Runner Double Play") then 0
    when (event="Sac Bunt Double Play") then 1
    when (event="Pickoff Error 3B") then 0
    when (event="etc") then 0
    when (event="Passed Ball") then 0
    when (event="Strikeout Triple Play") then 1
    when (event="Bunt Lineout") then 1
    when (event="Pickoff Error 2B") then 0
    when (event="Stolen Base 3B") then 1
    when (event="Pickoff Error 1B") then 0
    when (event="Field Out") then 1
    when (event="Defensive Indiff") then 0
    when (event="Injury") then 0
    when (event="Other Advance") then 0
    when (event="Stolen Base Home") then 0
    when (event="Game Advisory") then 0
    when (event="Error") then 0
    when (event="Ejection") then 0
    when (event="Offensive Substitution") then 0
    when (event="Cs Double Play") then 1
    else 1
  end
into _pa
from new_event_player
where player_type="pitcher";

INSERT INTO event_pitchers (player_uid, season, event, opponent_hand, count, is_hit, at_bat, pa)
values (_player_uid, _season,  event,_opponent_hand, _count, _is_hit, _at_bat, _pa);

END;
$$
DELIMITER ;

call test1(110003, 1977);
```

2
```python
drop procedure if exists test1;



DELIMITER $$
create procedure TEST1(_player_uid INT, _season INT)
BEGIN
	declare _opponent_hand varchar(10);
    declare _count int;
	declare _is_hit bit(1);
    declare _at_bat bit(1);
    declare _pa bit(1);
    declare _event varchar(60);

select
  case
    when (event="Walk") then 0
    when (event="Single") then 1
    when (event="Forceout") then 0
    when (event="Groundout") then 0
    when (event="Flyout") then 0
    when (event="Strikeout") then 0
    when (event="Double") then 1
    when (event="Lineout") then 0
    when (event="Sac Bunt") then 0
    when (event="Bunt Pop Out") then 0
    when (event="Home Run") then 0
    when (event="Pop Out") then 0
    when (event="Field Error") then 0
    when (event="Hit By Pitch") then 1
    when (event="Grounded Into DP") then 0
    when (event="Caught Stealing 2B") then 0
    when (event="Triple") then 0
    when (event="Intent Walk") then 0
    when (event="Fielders Choice Out") then 0
    when (event="Bunt Groundout") then 0
    when (event="Double Play") then 0
    when (event="Sac Fly") then 0
    when (event="Pickoff Caught Stealing 2B") then 0
    when (event="Strikeout Double Play") then 0
    when (event="Caught Stealing Home") then 0
    when (event="Fielders Choice") then 0
    when (event="Runner Out") then 0
    when (event="Pickoff 1B") then 0
    when (event="Sac Fly Double Play,") then 0
    when (event="Batter Out") then 0
    when (event="Caught Stealing 3B") then 0
    when (event="Wild Pitch") then 0
    when (event="Pickoff Caught Stealing 3B") then 0
    when (event="Pickoff 2B") then 0
    when (event="Pickoff 3B") then 0
    when (event="Pickoff Caught Stealing Home") then 0
    when (event="Catcher Interference") then 0
    when (event="Pitching Substitution") then 0
    when (event="Stolen Base 2B") then 0
    when (event="Triple Play") then 0
    when (event="Defensive Switch") then 0
    when (event="Balk") then 0
    when (event="Defensive Sub") then 0
    when (event="Runner Double Play") then 0
    when (event="Sac Bunt Double Play") then 0
    when (event="Pickoff Error 3B") then 0
    when (event="etc") then 0
    when (event="Passed Ball") then 0
    when (event="Strikeout Triple Play") then 0
    when (event="Bunt Lineout") then 0
    when (event="Pickoff Error 2B") then 0
    when (event="Stolen Base 3B") then 0
    when (event="Pickoff Error 1B") then 0
    when (event="Field Out") then 0
    when (event="Defensive Indiff") then 0
    when (event="Injury") then 0
    when (event="Other Advance") then 0
    when (event="Stolen Base Home") then 0
    when (event="Game Advisory") then 0
    when (event="Error") then 0
    when (event="Ejection") then 0
    when (event="Offensive Substitution") then 0
    when (event="Cs Double Play") then 0
  end
into _is_hit
from new_event_player
where player_uid=_player_uid and season=_season and player_type="pitchers";

select 
  case
    when (event="Walk") then 0
    when (event="Single") then 1
    when (event="Forceout") then 1
    when (event="Groundout") then 1
    when (event="Flyout") then 1
    when (event="Strikeout") then 1
    when (event="Double") then 1
    when (event="Lineout") then 1
    when (event="Sac Bunt") then 0
    when (event="Bunt Pop Out") then 1
    when (event="Home Run") then 1
    when (event="Pop Out") then 1
    when (event="Field Error") then 0
    when (event="Hit By Pitch") then 1
    when (event="Grounded Into DP") then 1
    when (event="Caught Stealing 2B") then 0
    when (event="Triple") then 1
    when (event="Intent Walk") then 0
    when (event="Fielders Choice Out") then 0
    when (event="Bunt Groundout") then 1
    when (event="Double Play") then 1
    when (event="Sac Fly") then 0
    when (event="Pickoff Caught Stealing 2B") then 0
    when (event="Strikeout Double Play") then 1
    when (event="Caught Stealing Home") then 0
    when (event="Fielders Choice") then 0
    when (event="Runner Out") then 0
    when (event="Pickoff 1B") then 0
    when (event="Sac Fly Double Play,") then 1
    when (event="Batter Out") then 1
    when (event="Caught Stealing 3B") then 0
    when (event="Wild Pitch") then 0
    when (event="Pickoff Caught Stealing 3B") then 0
    when (event="Pickoff 2B") then 0
    when (event="Pickoff 3B") then 0
    when (event="Pickoff Caught Stealing Home") then 0
    when (event="Catcher Interference") then 0
    when (event="Pitching Substitution") then 0
    when (event="Stolen Base 2B") then 0
    when (event="Triple Play") then 1
    when (event="Defensive Switch") then 0
    when (event="Balk") then 0
    when (event="Defensive Sub") then 0
    when (event="Runner Double Play") then 0
    when (event="Sac Bunt Double Play") then 1
    when (event="Pickoff Error 3B") then 0
    when (event="etc") then 0
    when (event="Passed Ball") then 0
    when (event="Strikeout Triple Play") then 1
    when (event="Bunt Lineout") then 1
    when (event="Pickoff Error 2B") then 0
    when (event="Stolen Base 3B") then 1
    when (event="Pickoff Error 1B") then 0
    when (event="Field Out") then 1
    when (event="Defensive Indiff") then 0
    when (event="Injury") then 0
    when (event="Other Advance") then 0
    when (event="Stolen Base Home") then 0
    when (event="Game Advisory") then 0
    when (event="Error") then 0
    when (event="Ejection") then 0
    when (event="Offensive Substitution") then 0
    when (event="Cs Double Play") then 1
    else 1
  end
into _at_bat
from new_event_player
where player_uid=_player_uid and season=_season and player_type="pitchers";

select 
  case
    when (event="Walk") then 1
    when (event="Single") then 1
    when (event="Forceout") then 1
    when (event="Groundout") then 1
    when (event="Flyout") then 1
    when (event="Strikeout") then 1
    when (event="Double") then 1
    when (event="Lineout") then 1
    when (event="Sac Bunt") then 1
    when (event="Bunt Pop Out") then 1
    when (event="Home Run") then 1
    when (event="Pop Out") then 1
    when (event="Field Error") then 0
    when (event="Hit By Pitch") then 1
    when (event="Grounded Into DP") then 1
    when (event="Caught Stealing 2B") then 0
    when (event="Triple") then 1
    when (event="Intent Walk") then 1
    when (event="Fielders Choice Out") then 1
    when (event="Bunt Groundout") then 1
    when (event="Double Play") then 1
    when (event="Sac Fly") then 1
    when (event="Pickoff Caught Stealing 2B") then 0
    when (event="Strikeout Double Play") then 1
    when (event="Caught Stealing Home") then 0
    when (event="Fielders Choice") then 0
    when (event="Runner Out") then 0
    when (event="Pickoff 1B") then 0
    when (event="Sac Fly Double Play,") then 1
    when (event="Batter Out") then 1
    when (event="Caught Stealing 3B") then 0
    when (event="Wild Pitch") then 0
    when (event="Pickoff Caught Stealing 3B") then 0
    when (event="Pickoff 2B") then 0
    when (event="Pickoff 3B") then 0
    when (event="Pickoff Caught Stealing Home") then 0
    when (event="Catcher Interference") then 1
    when (event="Pitching Substitution") then 0
    when (event="Stolen Base 2B") then 0
    when (event="Triple Play") then 1
    when (event="Defensive Switch") then 0
    when (event="Balk") then 0
    when (event="Defensive Sub") then 0
    when (event="Runner Double Play") then 0
    when (event="Sac Bunt Double Play") then 1
    when (event="Pickoff Error 3B") then 0
    when (event="etc") then 0
    when (event="Passed Ball") then 0
    when (event="Strikeout Triple Play") then 1
    when (event="Bunt Lineout") then 1
    when (event="Pickoff Error 2B") then 0
    when (event="Stolen Base 3B") then 1
    when (event="Pickoff Error 1B") then 0
    when (event="Field Out") then 1
    when (event="Defensive Indiff") then 0
    when (event="Injury") then 0
    when (event="Other Advance") then 0
    when (event="Stolen Base Home") then 0
    when (event="Game Advisory") then 0
    when (event="Error") then 0
    when (event="Ejection") then 0
    when (event="Offensive Substitution") then 0
    when (event="Cs Double Play") then 1
    else 1
  end
into _pa
from new_event_player
where player_uid=_player_uid and season=_season and player_type="pitchers";

INSERT INTO event_pitchers (player_uid, season, event, opponent_hand, count, is_hit, at_bat, pa)
values (_player_uid, _season,  event,_opponent_hand, _count, _is_hit, _at_bat, _pa);

END;
$$
DELIMITER ;
select * from event_player;

call test1(110003, 1977);
```