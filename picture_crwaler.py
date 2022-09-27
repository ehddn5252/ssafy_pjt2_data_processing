from DB.DML import DML
from core.crwalingmlbimg import CrawlingMlbImg

if __name__ == "__main__":
    dml_instance = DML()

    table_name = "baseball_players"
    # select_list = ["uid", "full_name"]
    select_list = ["name_slug"]
    # players = dml_instance.get_select_from_where(column_names=select_list, table_name=table_name)
    condition = "img_url is null"
    players = dml_instance.get_select_from_where(column_names=select_list, table_name=table_name,condition=condition,print_sql=True)
    print(f'남은 크롤링할 선수 사진 수: {len(players)}')
    row_num1 = len(players) // 5
    row_num2 = len(players) // 5 * 2
    row_num3 = len(players) // 5 * 3
    row_num4 = len(players) // 5 * 4
    row_num5 = len(players)

    field_name = "img_url"
    # CrawlingMlbImg.save_db(table_name=table_name, player_names=players[now_num:row_num1], field_name=field_name)
    CrawlingMlbImg.save_db_by_name_slug(table_name=table_name, name_slugs=players[row_num4:row_num5], field_name=field_name)

    # 찬호
    # CrawlingGoogleImg.main(players[row_num1:row_num2])
    # 예림
    # CrawlingGoogleImg.main(players[row_num2:row_num3])
