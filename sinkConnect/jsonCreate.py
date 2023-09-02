import psycopg2
import json
import os
from dotenv import load_dotenv

def json_create_check(result, rule_json):
    if not rule_json:
        return False

    all_union_feilds = []
    # result 리스트 요소가 튜플로 되어 있어서 string으로 만들기 위한 작업
    for i in result:
        all_union_feilds.append(i[0])

    for i in rule_json['logic']:
        if i['field'] not in all_union_feilds:
            return False
    
    return True

# load_dotenv()

# DB 정보
# host = os.getenv('XI_SND_DB_HOST')
# dbname=os.getenv('XI_SND_DB_NAME')
# user=os.getenv('XI_SND_DB_USER')
# password=os.getenv('XI_SND_DB_PASSWORD')
# port=os.getenv('XI_SND_DB_PORT')


def json_create(session):

    # 테이블 이름
    json_table = 'gprtm.rule_manager' # json 저장하는 테이블
    integrated_table = "union_customer" # union 테이블
    # DB 연결
    try:
        
        if not (session):
            raise Exception("DB 연결 실패입니다. 접속한 IP, DB 정보 등을 확인해주세요.")

        # Union 테이블 필드를 모두 조회하는 쿼리        
        select_all_integrated_table = f"""select a.attname  as "colname"
        from
            pg_catalog.pg_class c
            inner join pg_catalog.pg_attribute a on a.attrelid = c.oid
        where
            c.relname = '{integrated_table}'
            and a.attnum > 0
            and a.attisdropped is false
            and a.attname not in ('seq', 'channel_tp', 'cust_seq', 'register_dt', 'modify_dt')
        order by a.attrelid, a.attnum;"""

        # Union 테이블 필드 조회
        query_result = session.execute(select_all_integrated_table)
        
        result = query_result.all()
        if not (result):
            raise Exception("Union 테이블 조회 실패입니다. 테이블 이름을 확인해주세요.")
        
        select_rule_json_json_table = f"SELECT rule_json FROM {json_table};"
        rule_json = session.execute(select_rule_json_json_table).all()[0][0]
        print(rule_json)
        
        if (json_create_check(result, rule_json)):
            print("다시 안 만들어도 돼")
            print("연결 종료")
            return True

        print("새로 또는 다시 만들어야 해")
        # 리턴 데이터 및 조건 정의
        dictionary_data = {'logic': []}
        conditions = {'name': [{"Source": ["CX", "CS", "DI"]}, {"Date": "new"}, {"Frquency": 1}],
                      'num': [{"Source": ["CX", "CS", "DI"]}, {"Date": "new"}, {"Frquency": 1}]}
        default_condition = {'condition': [{"Source": ["CX", "CS", "DI"]}, {"Date": "new"}, {"Frquency": 1}]}

        # 필드들을 반복문을 돌면서 conditions에 조건이 있으면 가져오고 없으면 default_condition으로 저장
        for i in result:
            field = {"field": i[0]}
            field['conditions'] = conditions[i] if conditions.get(i) else default_condition['condition']
            dictionary_data['logic'].append(field)

        
        json_data = json.dumps(dictionary_data)
        json_default_condition = json.dumps(default_condition)

        # 로직 논의 필요 현재는 1개의 레코드만 사용하는 방식으로 사용중
        # 로그처럼 밑으로 늘려가는 방식도 사용 가능
        # 만약 json_table에 레코드가 없다면 insert 있으면 update
        insert_json = f"INSERT INTO {json_table} (seq, rule_json, default_json) VALUES (1, '{json_data}', '{json_default_condition}');"
        update_json = f"UPDATE {json_table} SET rule_json='{json_data}', default_json='{json_default_condition}';"
        select_all_json_table = f"SELECT * FROM {json_table};"

        tf_result = session.execute(select_all_json_table).all()[0][0]
        execute_query = update_json if tf_result else insert_json
        session.execute(execute_query)
        session.commit()

    except Exception as e :
        print(e)
        print("연결 종료") 
        return False

    print("연결 종료")
    return True
