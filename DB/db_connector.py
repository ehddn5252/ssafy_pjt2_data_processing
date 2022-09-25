# STEP 1
import pymysql
from asset.DB_info import db_infos, local_db_infos, e202_db_infos
class DB_controller:
    con = pymysql.connect(host=db_infos.host, user=db_infos.user, password=db_infos.password,
                          db=db_infos.db, charset='utf8')  # 한글처리 (charset = 'utf8')

    local_con = pymysql.connect(host=local_db_infos.host, user=local_db_infos.user, password=local_db_infos.password,
                          db=local_db_infos.db, charset='utf8')  # 한글처리 (charset = 'utf8')

    e202_con = pymysql.connect(host=e202_db_infos.host, user=e202_db_infos.user, password=e202_db_infos.password,
                          db=e202_db_infos.db, charset='utf8')  # 한글처리 (charset = 'utf8')