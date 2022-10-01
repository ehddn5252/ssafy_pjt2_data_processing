from DB.DML import DML
from typing import List
# 작성일 2022.09.30


from data.info_data import new_event_to_pitchers_dict, pitcher_primary_position_abbreviation


# raw_data는 튜플형태

def update(table_name: str, where_condition: List, set_value: str):
    where = ""
    for i, event in enumerate(where_condition):
        if i != 0:
            where += f" or event='{event}'"
        else:
            where += f" where event='{event}'"
    sql = f"update new_event_pitchers set {set_value} = 1"
    sql += where
    # sql = f"update event_pitchers set {set_value} = 0" # reset
    dml_instance.execute_update_sql(sql)
    print(sql)

if __name__ == "__main__":

    dml_instance = DML()
    is_hit_list = []
    at_bat_list = []
    pa_list = []
    for key, value in new_event_to_pitchers_dict.items():
        if value[0] == 1:
            is_hit_list.append(key)
        if value[1] == 1:
            at_bat_list.append(key)
        if value[2] == 1:
            pa_list.append(key)
    print(new_event_to_pitchers_dict)
    table_name = "new_event_pitchers"
    update(table_name, is_hit_list, "is_hit")
    update(table_name, at_bat_list, "at_bat")
    update(table_name, pa_list, "pa")
