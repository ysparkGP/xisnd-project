import json

def json_create_check(union_table_fields, rule_json):
    # rule_json이 존재하지 않는다면 다시 생성해야 함
    if not rule_json:
        return False

    all_union_feilds = set()
    rule_json_fields = set()

    # result 리스트 요소가 튜플로 되어 있어서 string으로 만들기 위한 작업
    for i in union_table_fields:
        if i[0] == 'uuid':
            continue
        all_union_feilds.add(i[0])

    for i in rule_json['logic']:
        rule_json_fields.add(i['field'])
    
    # 만약 rule_json 필드에 union 필드가 없다면 다시 실행해야함
    if all_union_feilds - rule_json_fields:
        return False

    return True


def json_create(session):

    # 테이블 이름
    json_table = 'gprtm.rule_manager' # json 저장하는 테이블
    unify_table = "unify_customer" # union 테이블
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
            c.relname = '{unify_table}'
            and a.attnum > 0
            and a.attisdropped is false
            and a.attname not in ('seq', 'channel_tp', 'cust_seq', 'register_dt', 'modify_dt')
        order by a.attrelid, a.attnum;"""

        # Union 테이블 필드 조회
        union_table_fields = session.execute(select_all_integrated_table).all()
        
        if not (union_table_fields):
            raise Exception("Union 테이블 조회 실패입니다. 테이블 이름을 확인해주세요.")
        
        select_rule_manager_qurey = f"SELECT rule_json, default_json, condition_json FROM {json_table};"
        json_result = session.execute(select_rule_manager_qurey).all()[0]
        rule_json = json_result[0] if json_result else {'logic': []}
        default_json = json_result[1] if json_result else {'condition': [{"Date": "new"},{"Frequency": 1},{"Source": ["DI", "OM", "CX", "CS"]}]}
        condition_json = json_result[2] if json_result else {}
        
        if (json_create_check(union_table_fields, rule_json)):
            return True

        # 리턴 데이터 및 조건 정의
        dictionary_data = {'logic': []}

        # 필드들을 반복문을 돌면서 conditions에 조건이 있으면 가져오고 없으면 default_condition으로 저장
        for i in union_table_fields:
            if i[0] == 'uuid':
                continue
            field = {"field": i[0]}
            field['conditions'] = condition_json[i[0]] if condition_json.get(i[0]) else default_json['condition']
            dictionary_data['logic'].append(field)

        
        json_data = json.dumps(dictionary_data)
        json_default_condition = json.dumps(default_json)
        json_condition = json.dumps(condition_json)

        # 로직 논의 필요 현재는 1개의 레코드만 사용하는 방식으로 사용중
        # 로그처럼 밑으로 늘려가는 방식도 사용 가능
        # 만약 json_table에 레코드가 없다면 insert 있으면 update
        insert_json = f"INSERT INTO {json_table} (seq, rule_json, default_json, condition_json) VALUES (1, '{json_data}', '{json_default_condition}', '{json_condition}');"
        update_json = f"UPDATE {json_table} SET rule_json='{json_data}';"
        select_all_json_table = f"SELECT * FROM {json_table};"

        tf_result = session.execute(select_all_json_table).all()[0][0]
        execute_query = update_json if tf_result else insert_json
        session.execute(execute_query)
        session.commit()

    except Exception as e :
        print(e)
        print("연결 종료") 
        return False

    # print("연결 종료")
    return True
