import psycopg2
from pytz import timezone
from dateutil.relativedelta import relativedelta
from datetime import datetime
import os

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
    #delete
    def deleteDB(self,schema,table,condition):
        sql = " delete from {schema}.{table} where {condition} ; ".format(schema=schema,table=table,
        condition=condition)
        try :
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print( "delete DB err", e)
    
if __name__ == "__main__":
    #클래스 호출
    db_dml = DML()
    
    #오늘 날짜
    today = datetime.now(timezone('Asia/Seoul'))
    today_d = today.strftime('%Y-%m-%d')

    #어제 날짜
    yesterday = today - relativedelta(days=1)
    yesterday_d = yesterday.strftime('%Y-%m-%d')

    db_dml.deleteDB("gprtm","task_manager",f"modify_dt < '{yesterday_d}' and lv2_tf = true")

