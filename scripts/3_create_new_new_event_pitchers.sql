
-- 원본 새 new_event_player 만들기
insert into new_event_player(player_uid, season, player_type, opponent_hand, event, count)
select player_uid,season,player_type, opponent_hand,event, count(uid)
from new_events
group by player_uid, season, player_type, opponent_hand, event;


-- 조정안: new_event_player
insert into new_event_player(player_uid, season, player_type, opponent_hand, event, count, strikes, balls, outs, rbi)
select player_uid,season,player_type, opponent_hand,event, count(uid), strikes, balls, outs, rbi
from new_events
where player_type="pitchers"
group by player_uid, season, player_type, opponent_hand, event;


-- 기존 event_pitchers 생성
insert into event_pitcher(player_uid,season,opponent_hand,event,event_type,count, is_hit, at_bat, pa)
select _player_uid, _season, _opponent_hand, _event, _event_type, _count, _is_hit, _at_bat, _pa,
from event_player
where player_type="pitchers"

-- 조정안: 새로운  event_pitcher
insert into new_event_pitchers(player_uid, season, opponent_hand, event, count, strikes, balls, outs, rbi)
select player_uid,season, opponent_hand,event, count(uid), sum(strikes), sum(balls), sum(outs), sum(rbi)
from new_events
group by player_uid, player_type,season, opponent_hand, event
having player_type="pitchers"


-- 2022.10.02 조정안2: 새로운  event_pitcher 10분
insert into new_new_event_pitchers(player_uid, season, opponent_hand, event, count, strikes, balls, outs, rbi, name, team_id, team_name)
select player_uid,season, opponent_hand,event, count(uid), sum(strikes), sum(balls), sum(outs), sum(rbi), name, team_id, team_name
from new_new_events
group by player_uid, player_type,season, opponent_hand,name,team_id,team_name, event
having player_type="pitchers"