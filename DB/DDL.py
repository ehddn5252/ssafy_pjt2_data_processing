from .db_connector import DB_controller
from data.sql_data import create_events_sql,create_event_pitchers_sql,create_pitchers_sql,create_test_sql

class DDL:
    conn = DB_controller.e202_con

    def create_table(self, table_name: str=""):
        if table_name == "pitchers":
            sql = create_pitchers_sql
        elif table_name == "event_pitchers":
            sql = create_event_pitchers_sql
        elif table_name == "events":
            sql = create_events_sql
        else:
            sql = create_test_sql
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()

    def drop_table(self, table_name:str):
        sql = f"DROP TABLE IF EXISTS `{table_name}`"
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()
