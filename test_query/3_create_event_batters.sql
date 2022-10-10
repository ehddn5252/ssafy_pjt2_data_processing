insert into new_event_batters (player_uid, season, opponent_hand,event,count)
select player_uid, season, opponent_hand, event, count(uid)
from new_events
where player_type = "batters"
group by player_uid, season, opponent_hand, event;