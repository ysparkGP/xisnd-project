import psycopg2
import os
import time 

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
    db_dml = DML()
    #코드 실행 시간
    current_time = time.time()
    #최대 대기 시간
    max_wait_time = 3 * 60 * 60

    db_dml.executeFunction(f"lv2.func_hc_apartment_complex()")
    db_dml.executeFunction(f"lv2.func_lv2_to_hc_apartment_complex()")

    pendnig_cnt = 1
    while pendnig_cnt != 0:
        pendnig_cnt = db_dml.readDB('hcsandbox','apartmentcomplex__c','count(*) as cnt','where "_hc_lastop" in (\'PENDING\',\'UPDATED\',\'INSERTED\')')[0][0]
        print(pendnig_cnt)
        if current_time >= (current_time + max_wait_time):
            break
        time.sleep(5)
    
    #while 조건문 깨진 후 실행
    db_dml.executeFunction(f"lv2.func_hc_apartment_room_type()")
    db_dml.executeFunction(f"lv2.func_hc_apartment()")
    db_dml.executeFunction(f"lv2.func_lv2_to_hc_apartment_room_type()")
    db_dml.executeFunction(f"lv2.func_lv2_to_hc_apartment()")

    pendnig_cnt = 1
    while pendnig_cnt != 0:
        pendnig_cnt = db_dml.readDB('hcsandbox','apartment__c','count(*) as cnt','where "_hc_lastop" in (\'PENDING\',\'UPDATED\',\'INSERTED\')')[0][0]
        print(pendnig_cnt)
        if current_time >= (current_time + max_wait_time):
            break
        time.sleep(5)

    #while 조건문 깨진 후 실행
    db_dml.executeFunction(f"lv2.func_hc_apartment_owner()")
    db_dml.executeFunction(f"lv2.func_lv2_to_hc_apartment_owner()")