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

def reverse_comparator(a,b):
    if a.cust_modify_dt >= b.cust_modify_dt : return -1
    elif a.cust_modify_dt < b.cust_modify_dt : return 1

def comparator(a,b):
    if a.cust_modify_dt <= b.cust_modify_dt : return -1
    elif a.cust_modify_dt > b.cust_modify_dt : return 1
    
def source_priority(li, sources):
    result_li = []
    for source in sources:
        for record in li:
            if record.channel_tp == source:
                result_li.append(record)
        if len(result_li) > 0 : break
    return result_li

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

def frequency_priority(li, column_name, condition_value):
    column = getattr(gprtm.Union_customer, column_name)
    frequency_dict = {}
    cnt = 0 if condition_value else 999999
    
    if condition_value == 1:
        for record in li:
            column_value = getattr(record, column_name)
            frequency_dict[column_value] = frequency_dict.get(column_value,0) + 1
            if cnt < frequency_dict[column_value]:
                cnt = frequency_dict[column_value]

    else :
        for record in li:
            column_value = getattr(record, column_name)
            frequency_dict[column_value] = frequency_dict.get(column_value,0) + 1
            if cnt > frequency_dict[column_value]:
                cnt = frequency_dict[column_value]

    result_li = []
    for record in li:
        if frequency_dict[getattr(record, column_name)] == cnt:
            result_li.append(record)
    return result_li
    
def decide_priority(session, last_unified_record, seq, uuid_param, table_tp):
    try:
        query = ' select rule_json from gprtm.rule_manager; '
        json = session.execute(query).all()[0][0]
        new_unify = []

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
        print(len(original_unify_priorities))
        for logic in json['logic']:
            field = logic['field']
            conditions = logic['conditions']
            
            if (table_tp == 'I') or (table_tp == 'H' and field == 'cust_marketing_tf'):
                temp_unify_priorities = []
                if field == 'cust_marketing_tf':
                    for original_unify_priority in marketing_unify_priorities:
                        temp_unify_priorities.append(copy.deepcopy(original_unify_priority))
                else:
                    for original_unify_priority in original_unify_priorities:
                        temp_unify_priorities.append(copy.deepcopy(original_unify_priority))

                unify_priorities = []
                for unify in temp_unify_priorities:
                    if getattr(unify, field) is not None and getattr(unify,'cust_modify_dt') is not None:
                        unify_priorities.append(unify)

                if len(unify_priorities) > 1:
                    for condition in conditions:
                        if len(unify_priorities) <= 1: break
                        condition_name = list(condition.keys())[0]
                        if condition_name == 'Source':
                            unify_priorities = source_priority(unify_priorities, condition[condition_name])
                        elif condition_name == 'Date':
                            unify_priorities = time_priority(unify_priorities, condition[condition_name])
                        elif condition_name == 'Frequency':
                            unify_priorities = frequency_priority(unify_priorities, field, condition[condition_name])

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
                last_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==tobe_uuid).first()
                if last_unified_record:
                    decide_priority(session, last_unified_record, seq, tobe_uuid, table_tp)
                    if unify_is_not_existed(last_unified_record):
                        session.delete(last_unified_record)
                        
                else :
                    last_unified_record = gprtm.Unify_customer(tobe_uuid)
                    if last_unified_record:
                        decide_priority(session, last_unified_record, seq, tobe_uuid, table_tp)

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
                if asis_uuid == tobe_uuid: 
                    last_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==tobe_uuid).first()
                    if last_unified_record:
                        decide_priority(session, last_unified_record, seq, tobe_uuid, table_tp)
                        if unify_is_not_existed(last_unified_record):
                            session.delete(last_unified_record)

                else:
                    asis_last_link_unified_record = session.query(gprtm.Unify_customer)\
                                                    .filter(gprtm.Unify_customer.uuid==asis_uuid,
                                                            gprtm.Unify_customer.uuid==gprtm.Link_customer.uuid)\
                                                    .first()
                    tobe_last_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==tobe_uuid).first()

                    if asis_last_link_unified_record : 
                        asis_last_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==asis_uuid).first()
                        if asis_last_unified_record: 
                            decide_priority(session, asis_last_unified_record, seq, asis_uuid, table_tp)

                            if unify_is_not_existed(asis_last_unified_record):
                                session.delete(asis_last_unified_record)
                    
                    else : 
                        asis_last_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==asis_uuid).first()
                        if asis_last_unified_record:
                            session.delete(asis_last_unified_record)

                    if tobe_last_unified_record :
                        decide_priority(session, tobe_last_unified_record, seq, tobe_uuid, table_tp)
                        if unify_is_not_existed(tobe_last_unified_record):
                            session.delete(tobe_last_unified_record)
                        
                    else :
                        last_unified_record = gprtm.Unify_customer(tobe_uuid)
                        if last_unified_record:
                            decide_priority(session, last_unified_record, seq, tobe_uuid, table_tp)
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


            elif task_dml_tp == 'DELETE':
                last_link_record = session.query(gprtm.Link_customer).filter(gprtm.Link_customer.uuid==asis_uuid).first()
                unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid == asis_uuid).first()
                if last_link_record :
                    if unified_record:
                        decide_priority(session, unified_record, seq, asis_uuid, table_tp)
                        if unify_is_not_existed(unified_record):
                            session.delete(unified_record)
                else :
                    if unified_record :
                        session.delete(unified_record)

            if changed_effected_uuids != None and len(changed_effected_uuids) > 0:
                for changed_effected_uuid in changed_effected_uuids:
                    changed_unified_record = gprtm.Unify_customer(changed_effected_uuid)
                    if changed_unified_record:
                        decide_priority(session, changed_unified_record, seq, changed_effected_uuid, table_tp)
                        if not unify_is_not_existed(changed_unified_record):
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