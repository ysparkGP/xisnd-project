import psycopg2
from pytz import timezone
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import os

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

class DDL(Databases):
    #테이블 생성
    def createTB(self,schema,table,startDate,endDate):
        table_date = startDate.replace("-","")
        sql = " CREATE TABLE IF NOT EXISTS \"{schema}\".\"{table}_{table_date}\" PARTITION OF \"{schema}\".\"{table}\" FOR VALUES FROM ('{startDate}') TO ('{endDate}');".format(schema=schema,table=table,startDate=startDate,endDate=endDate,table_date=table_date)
        try:
            print(sql)
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            self.db.rollback()
            print(" create TB err ",e) 

    #테이블 드랍
    def dropTB(self,schema,table,date):
        date = date.replace(",","")
        sql = " DROP TABLE IF EXISTS \"{schema}\".\"{table}_{date}\";".format(schema=schema,table=table,date=date)
        try :
            print(sql)
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print( "drop TB err", e)

    def dropTB_sql(self,schema,table):
        sql = " DROP TABLE IF EXISTS \"{schema}\".\"{table}\";".format(schema=schema,table=table)
        try :
            print(sql)
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print( "drop TB err", e)

class DML(Databases):
    #select
    def readDB(self,schema,table,colum,condition):
        sql = " SELECT {colum} from \"{schema}\".\"{table}\" {condition}".format(colum=colum,schema=schema,table=table,condition=condition)
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
        except Exception as e :
            result = (" read DB err",e)
        return result
    
if __name__ == "__main__":
    #클래스 호출
    db_ddl = DDL()
    db_dml = DML()
    
    #오늘 날짜
    today = datetime.now(timezone('Asia/Seoul'))
    
    #미래 1주
    plus_one_weeks = today + relativedelta(weeks=1)

    #존재하는 테이블 조회
    for i in db_dml.readDB(schema='gprtm',table='partition_manager',colum='"schema_nm","table_nm","store_d_num"',condition='where partition_tf = ''true'''):
        create_start_date = today - relativedelta(days=i[2])

        for table in db_dml.readDB(schema='pg_catalog',table='pg_tables',colum='"tablename"',condition="where schemaname = '" + i[0] + "' and tablename like '" + i[1] +"_%'"):
            last_underscore_index = table[0].rfind('_')

            # 마지막 '_' 뒤의 8글자 숫자를 추출합니다.
            if last_underscore_index != -1 and len(table[0]) - last_underscore_index == 9:
                last_8_digits = table[0][last_underscore_index + 1:]
                if last_8_digits.isdigit():
                    table_date = datetime.strptime(last_8_digits, '%Y%m%d').replace(tzinfo=timezone('Asia/Seoul'))
                    if table_date < create_start_date - relativedelta(days=1) :
                        # 테이블 삭제 SQL 실행
                        db_ddl.dropTB_sql(i[0], table[0])

        for single_date in daterange(create_start_date, plus_one_weeks):
            # 일 간격으로 날짜 잘라서 추출 (추후 일,주,월,년 단위로 조건걸어야함)
            tomorrow = single_date + relativedelta(days=1)
            today_str = single_date.strftime('%Y-%m-%d')  # YYYY-MM-DD 형식으로 변경
            tomorrow_str = tomorrow.strftime('%Y-%m-%d')  # YYYY-MM-DD 형식으로 변경
            # 테이블 생성
            db_ddl.createTB(i[0], i[1], today_str, tomorrow_str)