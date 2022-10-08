from DB.DML import DML

if __name__ == "__main__":

    dml_instance = DML()
    # dml_instance.execute_sql()
    s ='''insert into new_event_batters (player_uid, season, opponent_hand)
        select player_uid, season, opponent_hand, event, count(*)
        from new_events
        where player_type = "batters"
        group by player_uid, season, opponent_hand, event;
        '''
    print(s)
    dml_instance.execute_sql(s)