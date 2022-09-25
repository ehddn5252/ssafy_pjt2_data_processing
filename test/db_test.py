# STEP 1
import pymysql

host = "stg-yswa-kr-practice-db-master.mariadb.database.azure.com"
user = "S07P12E103@stg-yswa-kr-practice-db-master.mariadb.database.azure.com"
db = 's07p12e103'
password = "BltqBnSpBc"

# STEP 2: MySQL Connection 연결
info = pymysql.connect(host=host, user=user, password=password,
                       db=db, charset='utf8')  # 한글처리 (charset = 'utf8')

# STEP 3: Connection 으로부터 Cursor 생성
cur = info.cursor()

# STEP 4: SQL문 실행 및 Fetch
sql = "SELECT * from people where id=110001"
cur.execute(sql)

# 데이타 Fetch
rows = cur.fetchall()
print(rows)  # 전체 rows

# STEP 5: DB 연결 종료
info.close()