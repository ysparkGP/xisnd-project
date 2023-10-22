import psycopg2
from pytz import timezone
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta, time
import os

mart_array = ['"gprtm_mart"."func_gprtm_to_mart"'] 

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

class Databases():
    def __init__(self):
        self.db = psycopg2.connect(host=os.environ['db_host'], dbname=os.environ['db_name'],user=os.environ['db_user'],password=os.environ['db_password'],port=os.environ['db_port'])
        self.cursor = self.db.cursor()

    def __del__(self):
        self.db.close()
        self.cursor.close()

    def execute(self,query,args={}):
        self.cursor.execute(query,args)
        row = self.cursor.fetchall()
        return row

    def commit(self):
        self.cursor.commit()

class DML(Databases):
    #execute function
    def executeFunction(self,function):
        sql = f"SELECT {function}"
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            print(e)
            self.db.rollback()
    
if __name__ == "__main__":
    #클래스 호출
    db_dml = DML()
    
    #오늘 날짜
    today = datetime.now(timezone('Asia/Seoul'))
    today_d = today.strftime('%Y-%m-%d')

    #어제 날짜
    yesterday = today - relativedelta(days=1)
    yesterday_d = yesterday.strftime('%Y-%m-%d')

    # 현재 시간 가져오기
    current_time = datetime.now(timezone('Asia/Seoul')).time()

    for i in mart_array:
        print(f"{i}('{today_d}')")
        db_dml.executeFunction(f"{i}('{today_d}')")
        # 현재 시간이 00:00 ~ 02:00 사이인지 확인 -> 이 경우 어제 함수도 재실행
        if current_time >= time(0, 0) and current_time < time(2, 0):
            print(f"{i}('{yesterday_d}')")
            db_dml.executeFunction(f"{i}('{yesterday_d}')")
        db_dml.executeFunction(f"lv2.func_hc_unify_statistics")
        db_dml.executeFunction(f"lv2.func_lv2_to_hc_unify_statistics")
