from .db_connector import DB_controller
from typing import List


class DML:
    conn = DB_controller.e202_con
    cur = conn.cursor()

    # def get_select_from_where(self, column_name: str, table_name: str, condition: str = ""):
    #
    #     # STEP 3: Connection 으로부터 Cursor 생성
    #     cur = self.conn.cursor()
    #     sql = ""
    #     # STEP 4: SQL문 설정
    #     if condition == "":
    #         sql = f"SELECT {column_name} from {table_name}"
    #     else:
    #         sql = f"SELECT {column_name} from {table_name} where {condition}"
    #
    #     cur.execute(sql)
    #
    #     # 데이타 Fetch
    #     rows = cur.fetchall()
    #     print(rows)  # 전체 rows
    #
    #     # STEP 5: DB 연결 종료
    #     self.conn.close()
    #     return rows

    def close(self):
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def fetch_all(self):
        return self.cur.fetchall();

    def execute(self, sql, vals=[]):
        if vals == []:
            self.cur.execute(sql)
        else:
            self.cur.execute(sql, vals)

    def execute_update_sql(self, sql, vals):
        self.cur.execute(sql, vals)
        self.conn.commit()

    def execute_insert_sql(self, sql, vals, is_print=False):
        if is_print:
            print(sql)

        self.cur.execute(sql, vals)
        self.conn.commit()

    def execute_fetch_sql(self, sql, vals, is_print=False):
        if is_print:
            print(sql)
        # cur = self.conn.cursor()
        self.cur.execute(sql, vals)
        fetched = self.cur.fetchall()

        return fetched

    def execute_insert_many_sql(self, sql, vals):
        # cur = self.conn.cursor()
        self.cur.executemany(sql, vals)
        self.conn.commit()

    def execute_sql(self, sql, is_print=False):
        if is_print:
            print(sql)
        # cur = self.conn.cursor()
        self.cur.execute(sql)
        self.conn.commit()

    def get_from_sql(self, sql):
        # cur = self.conn.cursor()
        self.cur.execute(sql)

        # 데이타 Fetch
        rows = self.cur.fetchall()
        return rows

    def get_select_from_where(self, column_names: List, table_name: str, condition: str = "", print_sql=False):

        # STEP 3: Connection 으로부터 Cursor 생성
        # cur = self.conn.cursor()
        sql = ""
        # STEP 4: SQL문 설정
        if condition == "":
            sql = f"SELECT "
            select_sql = ""
            for i in range(len(column_names)):
                if i == len(column_names) - 1:
                    select_sql += column_names[i] + " "
                else:
                    select_sql += column_names[i] + ", "
            sql += select_sql
            sql += f"from {table_name}"
        else:
            sql = f"SELECT "
            select_sql = ""
            for i in range(len(column_names)):
                if i == len(column_names) - 1:
                    select_sql += column_names[i] + " "
                else:
                    select_sql += column_names[i] + ", "
            sql += select_sql
            sql += f"from {table_name} "
            sql += f"where {condition}"

        if print_sql:
            print(sql)
        # sql = "SELECT id, fullName from people"
        # sql = f"SELECT {column_name, column_name2} from {table_name} where {condition}"

        self.cur.execute(sql)

        # 데이타 Fetch
        rows = self.cur.fetchall()
        # print(rows)  # 전체 rows
        return rows

    def update_from_where(self, table_name: str, field_name: str, data: str, condition: str = ""):

        # STEP 3: Connection 으로부터 Cursor 생성
        # cur = self.conn.cursor()
        sql = ""
        # STEP 4: SQL문 설정
        if condition != "":
            sql = f"update "
            sql += f"{table_name} "
            sql += f"SET {field_name} ='{data}' "
            sql += f"where {condition}"
        print(sql)
        self.cur.execute(sql)

        # 데이타 Fetch
        rows = self.cur.fetchall()
        # print(rows)  # 전체 rows
        self.conn.commit()
        # STEP 5: DB 연결 종료
        return rows
