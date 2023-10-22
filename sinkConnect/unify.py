from sqlalchemy import MetaData, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import os
from dotenv import load_dotenv
from model import gprtm_models as gprtm
import itertools
from collections import Counter
from functools import cmp_to_key
import copy

def make_unify_upsert_data(record):
    upsert_data = {
        'uuid' : record.uuid,
        'seq' : record.seq,
        'cust_nm' : record.cust_nm,
        'cust_email' : record.cust_email,
        'cust_phone' : record.cust_phone,
        'cust_addr' : record.cust_addr,
        'cust_addr_state' : record.cust_addr_state,
        'cust_addr_city' : record.cust_addr_city,
        'cust_birth_d' : record.cust_birth_d,
        'cust_register_dt' : record.cust_register_dt,
        'cust_modify_dt' : record.cust_modify_dt,
        'cust_gender_tp' : record.cust_gender_tp,
        'cust_marketing_tf' : record.cust_marketing_tf,
        'register_dt' : record.register_dt,
        'modify_dt' : record.modify_dt,
        'cust_hc_sfid' : record.cust_hc_sfid
    }

    return upsert_data

# 수정날짜 기준 커스텀 Comparator
def reverse_comparator(a,b):
    if a.cust_modify_dt >= b.cust_modify_dt : return -1
    elif a.cust_modify_dt < b.cust_modify_dt : return 1

def comparator(a,b):
    if a.cust_modify_dt <= b.cust_modify_dt : return -1
    elif a.cust_modify_dt > b.cust_modify_dt : return 1
    
# Source 기준 순차 삭제
def source_priority(li, sources):
    result_li = []
    for source in sources:
        for record in li:
            if record.channel_tp == source:
                result_li.append(record)
        if len(result_li) > 0 : break

    return result_li

# Date 정렬 및 순차 삭제
def time_priority(li, order):
    if order == 'new':
        li.sort(key=cmp_to_key(reverse_comparator))
    else :
        li.sort(key=cmp_to_key(comparator))
    
    result_li = []
    if li:
        value = li[0].cust_modify_dt
        for record in li:
            if record.cust_modify_dt == li[0].cust_modify_dt:
                result_li.append(record)
            else : break
    return result_li

# Frequency 기준 정렬 및 순차 삭제
def frequency_priority(li, column_name, condition_value):
    column = getattr(gprtm.Union_customer, column_name)
    frequency_dict = {}
    # condition_value = 1 이면 많은 순, 0 이면 적은 순
    cnt = 0 if condition_value else 999999
    
    # 내림차순
    if condition_value == 1:
        # 통합 기준 컬럼 값 빈도 사전 만들기
        for record in li:
            column_value = getattr(record, column_name)
            frequency_dict[column_value] = frequency_dict.get(column_value,0) + 1
            if cnt < frequency_dict[column_value]:
                cnt = frequency_dict[column_value]

    # 오름차순
    else :
        for record in li:
            column_value = getattr(record, column_name)
            frequency_dict[column_value] = frequency_dict.get(column_value,0) + 1
            if cnt > frequency_dict[column_value]:
                cnt = frequency_dict[column_value]
    # print(frequency_dict)
    result_li = []
    for record in li:
        if frequency_dict[getattr(record, column_name)] == cnt:
            result_li.append(record)
    return result_li
    

# 통합 컬럼 값 우선순위 진행
def decide_priority(session, last_unified_record, seq, uuid_param, table_tp):
    try:
        query = ' select rule_json from gprtm.rule_manager; '
        json = session.execute(query).all()[0][0]
        new_unify = []

        # 통합 컬럼 값을 정할 Union 레코드들 미리 뽑기
        original_unify_priorities = session.query(gprtm.Union_customer)\
                                            .filter(
                                                gprtm.Link_customer.uuid==uuid_param,
                                                gprtm.Link_customer.channel_tp==gprtm.Union_customer.channel_tp,
                                                gprtm.Link_customer.cust_seq==gprtm.Union_customer.cust_seq
                                            ).all()
        
        marketing_unify_priorities = session.query(gprtm.Marketing_customer)\
                                            .filter(
                                                gprtm.Link_customer.uuid==uuid_param,
                                                gprtm.Link_customer.channel_tp==gprtm.Marketing_customer.channel_tp,
                                                gprtm.Link_customer.cust_seq==gprtm.Marketing_customer.cust_seq
                                            ).all()
        for logic in json['logic']:
            field = logic['field']
            conditions = logic['conditions']
            
            if (table_tp == 'I') or (table_tp == 'H' and field == 'cust_marketing_tf'):
                # 쿼리 부하를 줄이기 위해 객체 깊은 복사를 썼지만 그렇게 빠른 방법은 아니기에 지켜봐야함
                temp_unify_priorities = []
                if field == 'cust_marketing_tf':
                    for original_unify_priority in marketing_unify_priorities:
                        temp_unify_priorities.append(copy.deepcopy(original_unify_priority))
                else:
                    for original_unify_priority in original_unify_priorities:
                        temp_unify_priorities.append(copy.deepcopy(original_unify_priority))

                # print(f'{original_unify_priorities[0].seq}')
                # 통합 컬럼 값이 NULL이 아닌 값들만 취급
                unify_priorities = []
                for unify in temp_unify_priorities:
                    if getattr(unify, field) is not None and getattr(unify,'cust_modify_dt') is not None:
                        unify_priorities.append(unify)
                # print(field)
                # for value in unify_priorities:
                #     print({getattr(value,'seq'), getattr(value,field)})

                if len(unify_priorities) > 1:
                    for condition in conditions:
                        if len(unify_priorities) <= 1: break
                        
                        condition_name = list(condition.keys())[0]
                        if condition_name == 'Source':
                            # print(f'---------------------{field} Source 정렬--------------------------')
                            unify_priorities = source_priority(unify_priorities, condition[condition_name])
                        elif condition_name == 'Date':
                            # print(f'---------------------{field} Date 정렬--------------------------')
                            unify_priorities = time_priority(unify_priorities, condition[condition_name])
                        elif condition_name == 'Frequency':
                            # print(f'---------------------{field} Frequency 정렬--------------------------')
                            unify_priorities = frequency_priority(unify_priorities, field, condition[condition_name])
                # print('통합 후 정렬')
                # for value in unify_priorities:
                #     print({getattr(value,'seq'), getattr(value,field)})
                # 통합할 컬럼 값이 존재하면 update
                if len(unify_priorities) > 0 : 
                    setattr(last_unified_record, field, getattr(unify_priorities[0], field))
                else : 
                    if field == 'cust_marketing_tf':
                        setattr(last_unified_record, field, False)
                    else:
                        setattr(last_unified_record, field, None)
    except Exception as e:
        print(e)
        raise Exception(e)
    
def unify_is_not_existed(unify_record):
    if unify_record.cust_nm is None: return True
    return False

def unify_start(session, seq, task_dml_tp, asis_uuid, tobe_uuid, effected_uuids, changed_effected_uuids, table_tp):
    try:
        if table_tp == 'I':

            if effected_uuids != None and len(effected_uuids) > 0:
                for effected_uuid in effected_uuids :
                    print('effected_uuid -> effected_uuid unify 제거 후 tobe_uuid unify reconciliation')
                    effected_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==effected_uuid).first()
                    if effected_unified_record:
                        session.delete(effected_unified_record)

            if task_dml_tp == 'INSERT':
                # Insert 시 unified_customer 테이블에 같은 uuid가 있는지 검색
                last_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==tobe_uuid).first()
                # Insert 시 새로운 통합 레코드가 생성되거나, 통합되어진 레코드에 끼거나
                if last_unified_record:
                    # 기존 레코드 Update
                    decide_priority(session, last_unified_record, seq, tobe_uuid, table_tp)

                    # link에 없어서 uuid를 제외한 모든 필드 값이 null로 채워진 경우 체크
                    if unify_is_not_existed(last_unified_record):
                        session.delete(last_unified_record)
                        
                else :
                    # 새로운 레코드 Insert
                    last_unified_record = gprtm.Unify_customer(tobe_uuid)
                    if last_unified_record:
                        # 멀티쓰레딩으로 인한 동시성 문제때문에 INSERT -> UPSERT
                        decide_priority(session, last_unified_record, seq, tobe_uuid, table_tp)

                        # link에 없어서 uuid를 제외한 모든 필드 값이 null로 채워진 경우 체크
                        if not unify_is_not_existed(last_unified_record):
                            upsert_data = make_unify_upsert_data(last_unified_record)
                            insert_stmt = insert(gprtm.Unify_customer).values(
                                upsert_data
                            )

                            do_update_stmt = insert_stmt.on_conflict_do_update(
                                constraint='unify_customer_un',
                                set_=upsert_data
                            )

                            session.execute(do_update_stmt)
                
            elif task_dml_tp == 'UPDATE':
                
                # Update 시 asis_uuid 와 tobe_uuid 를 비교 후, 같으면 tobe_uuid에만 통합진행, 다르면 asis, tobe 둘 다 진행
                if asis_uuid == tobe_uuid: 
                    last_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==tobe_uuid).first()
                    if last_unified_record:
                        decide_priority(session, last_unified_record, seq, tobe_uuid, table_tp)

                        # link에 없어서 uuid를 제외한 모든 필드 값이 null로 채워진 경우 체크
                        if unify_is_not_existed(last_unified_record):
                            session.delete(last_unified_record)

                else:
                    # asis_uuid 와 tobe_uuid 가 다른 경우,
                    # asis_uuid 의 통합 고객이 새로 업데이트 되어지고, tobe_uuid 의 통합 고객도 새로 업데이트 되어져야함
                    # 단, asis_uuid는 없어졌는지, 남아있는지를 확인해야함
                    # tobe_uuid 를 가진 unify 가 새로 생성될 수 있음
                    asis_last_link_unified_record = session.query(gprtm.Unify_customer)\
                                                    .filter(gprtm.Unify_customer.uuid==asis_uuid,
                                                            gprtm.Unify_customer.uuid==gprtm.Link_customer.uuid)\
                                                    .first()
                    tobe_last_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==tobe_uuid).first()

                    if asis_last_link_unified_record : 
                        print('asis_uuid unify 남아있어서 업데이트 해야함')
                        asis_last_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==asis_uuid).first()
                        if asis_last_unified_record: 
                            decide_priority(session, asis_last_unified_record, seq, asis_uuid, table_tp)

                            # link에 없어서 uuid를 제외한 모든 필드 값이 null로 채워진 경우 체크
                            if unify_is_not_existed(asis_last_unified_record):
                                session.delete(asis_last_unified_record)
                    
                    else : 
                        print('asis_uuid unify 제거해야함')
                        asis_last_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==asis_uuid).first()
                        if asis_last_unified_record:
                            session.delete(asis_last_unified_record)

                    if tobe_last_unified_record :
                        print('tobe_uuid unify 남아있어서 업데이트 해야함')
                        decide_priority(session, tobe_last_unified_record, seq, tobe_uuid, table_tp)

                        # link에 없어서 uuid를 제외한 모든 필드 값이 null로 채워진 경우 체크
                        if unify_is_not_existed(tobe_last_unified_record):
                            session.delete(tobe_last_unified_record)
                        
                    else :
                        print('tobe_uuid unify 없어서 새로 생성해야함')
                        last_unified_record = gprtm.Unify_customer(tobe_uuid)
                        if last_unified_record:
                            decide_priority(session, last_unified_record, seq, tobe_uuid, table_tp)

                            # link에 없어서 uuid를 제외한 모든 필드 값이 null로 채워진 경우 체크
                            if not unify_is_not_existed(last_unified_record):
                                # 멀티쓰레딩으로 인한 동시성 문제때문에 INSERT -> UPSERT
                                upsert_data = make_unify_upsert_data(last_unified_record)
                                insert_stmt = insert(gprtm.Unify_customer).values(
                                    upsert_data
                                )

                                do_update_stmt = insert_stmt.on_conflict_do_update(
                                    constraint='unify_customer_un',
                                    set_=upsert_data
                                )

                                session.execute(do_update_stmt)


            elif task_dml_tp == 'DELETE':
                
                # Delete 시 asis_uuid로만 판단하고, 기존 통합 레코드 업데이트나 삭제되는 경우
                # Delete 되어진 고객이 link_customer 에서 남아있는지 검색
                last_link_record = session.query(gprtm.Link_customer).filter(gprtm.Link_customer.uuid==asis_uuid).first()
                unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid == asis_uuid).first()
                # print(last_link_record)
                if last_link_record :
                    # 남아있다면 통합 레코드 업데이트
                    if unified_record:
                        decide_priority(session, unified_record, seq, asis_uuid, table_tp)

                        # link에 없어서 uuid를 제외한 모든 필드 값이 null로 채워진 경우 체크
                        if unify_is_not_existed(unified_record):
                            session.delete(unified_record)
                else :
                    # link에는 없지만 {unified_record.uuid}가 unify에 존재해서 삭제
                    if unified_record :
                        session.delete(unified_record)
                    # 없으면 기존에 남아있던 통합 레코드 제거
            

            if changed_effected_uuids != None and len(changed_effected_uuids) > 0:
                print('changed_effected_uuid -> asis_uuid가 바뀌었으니, asis_uuid_unify 제거 또는 변경(로직 이미 존재) 후 changed_uuid_unify insert, reconciliation')
                for changed_effected_uuid in changed_effected_uuids:
                    changed_unified_record = gprtm.Unify_customer(changed_effected_uuid)
                    if changed_unified_record:
                        decide_priority(session, changed_unified_record, seq, changed_effected_uuid, table_tp)
                        
                        # link에 없어서 uuid를 제외한 모든 필드 값이 null로 채워진 경우 체크
                        if not unify_is_not_existed(changed_unified_record):
                            # 멀티쓰레딩으로 인한 동시성 문제때문에 INSERT -> UPSERT
                            upsert_data = make_unify_upsert_data(changed_unified_record)
                            insert_stmt = insert(gprtm.Unify_customer).values(
                                upsert_data
                            )

                            do_update_stmt = insert_stmt.on_conflict_do_update(
                                constraint='unify_customer_un',
                                set_=upsert_data
                            )
                            session.execute(do_update_stmt)
        
        else:
            tobe_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==tobe_uuid).first()
            if tobe_unified_record :
                decide_priority(session, tobe_unified_record, seq, tobe_uuid, table_tp)
                
                # link에 없어서 uuid를 제외한 모든 필드 값이 null로 채워진 경우 체크
                if not tobe_unified_record:
                    decide_priority(session, tobe_unified_record, seq, changed_effected_uuid, table_tp)
                
                    upsert_data = make_unify_upsert_data(tobe_unified_record)
                    insert_stmt = insert(gprtm.Unify_customer).values(
                        upsert_data
                    )

                    do_update_stmt = insert_stmt.on_conflict_do_update(
                        constraint='unify_customer_un',
                        set_=upsert_data
                    )

                    session.execute(do_update_stmt)

        session.commit()
        return True
    
    except Exception as e:
        print(f'task_record {seq} 통합 과정 에러 발생 {e}')
        session.rollback()
        return False