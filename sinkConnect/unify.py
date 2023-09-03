from sqlalchemy import MetaData, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv
from model import gprtm_models as gprtm
import itertools
from collections import Counter


def decide_priority(session, last_unified_record, seq, uuid_param):
    # 통합 룰 검색 후 'logic' 
    query = ' select rule_json from gprtm.rule_manager; '
    json = session.execute(query).all()[0][0]
    new_unify = []
    for logic in json['logic']:
        # print(logic)
        field = logic['field']
        # 멤버 변수 동적으로 불러오기
        value = getattr(gprtm.Union_customer, field)
        conditions = logic['conditions']
        unify_priorities = []
        
        isExit = False
        for condition in conditions:
            if isExit : break

            condition_name = list(condition.keys())[0]
            # step 1. 'Source'
            # Source 단계가 첫 번째가 되어야 함.
            # Source 단계에서 후보군들을 찾아 리스트 삽입
            if condition_name == 'Source':
                source_priorities = condition[condition_name]
                # print(source_priorities)
                for source_priority in source_priorities:
                    # 건네받은 uuid를 조건으로, link와 union을 조인시켜 Union_customer 검색
                    temp_list = [session.query(gprtm.Union_customer)\
                                        .filter(
                                            gprtm.Link_customer.uuid==uuid_param,
                                            gprtm.Link_customer.channel_tp == gprtm.Union_customer.channel_tp,
                                            gprtm.Link_customer.cust_seq == gprtm.Union_customer.cust_seq,
                                            gprtm.Link_customer.channel_tp == source_priority,
                                            # gprtm.Union_customer.cust_nm.isnot(None)
                                            value.isnot(None)
                                        #  f'gprtm.Union_customer.{field}.isnot(None)'
                                        ).all()
                                        # .join(
                                        #     gprtm.Link_customer, 
                                        #     gprtm.Union_customer.channel_tp == gprtm.Link_customer.channel_tp,
                                        #     gprtm.Union_customer.cust_seq == gprtm.Link_customer.cust_seq
                                        # )\
                                ]
                    # Source 우선순위대로 1차원 리스트 생성
                    # 위 쿼리에서 나온 결과물이 없으면 넘어감
                    if len(temp_list) >= 1:
                        unify_priorities+=(temp_list)
                        # Source 우선순위만 보고 끝나는 상황
                        if len(temp_list) == 1:
                            unify_priorities = list(itertools.chain(*unify_priorities)) # flatten
                            isExit = True
                            break 

                # 통합 요소 없음
                if len(unify_priorities) == 0:
                    break
        
            # step 2. 'Date'
            elif condition_name == 'Date':
                # Union_customer 의 cust_modify_dt 내림차순 정렬
                # Step1 을 거쳐 만들어진 unify_priorities(채널소스 우선순위대로 쌓인, 길이가 2이상인 2차원 배열) 을 정렬 및 2차원 -> 1차원
                unify_priorities[0].sort(key=lambda object: object.cust_modify_dt, reverse=True)
                unify_priorities = list(itertools.chain(*unify_priorities)) # flatten
                if unify_priorities[0].cust_modify_dt != unify_priorities[1].cust_modify_dt:
                    break

            # step 3. 'Frequency'
            elif condition_name == 'Frequency':
                # 각 채널들 전부에서 카운트 하는건지, 채널을 기준으로 독립적이게 카운트 하는건지?
                # 만약 우선순위 2 CX = [3,4,3], 우선순위 3 DI = [4,4] 라면 3번,4번 중 어느 것이 뽑혀야하는건지?
                continue
        

        print(f'--------------------{field}---------------------')
        print(unify_priorities)
        # for unify_priority in unify_priorities:
            # print(unify_priority)
            # print(f'{unify_priority.channel_tp}, {unify_priority.cust_seq}, {unify_priority.cust_nm} ')
            
        value = unify_priorities[0] if len(unify_priorities) > 0 else None
        setattr(last_unified_record, field, value)


def unify_start(session, seq, task_dml_tp, asis_uuid, tobe_uuid):
    # Insert
    try:
        if task_dml_tp == 'INSERT':

            # Insert 시 unified_customer 테이블에 같은 uuid가 있는지 검색
            last_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==tobe_uuid).first()
            # Insert 시 새로운 통합 레코드가 생성되거나, 통합되어진 레코드에 끼거나
            if last_unified_record:
                # 기존 레코드 Update
                decide_priority(session, last_unified_record, seq, tobe_uuid)
            else :
                # 새로운 레코드 Insert
                last_unified_record = gprtm.Unify_customer(tobe_uuid)
                decide_priority(session, last_unified_record, seq, tobe_uuid)
                session.add(last_unified_record)
            
        # Update   
        elif task_dml_tp == 'UPDATE':
            
            # Update 시 asis_uuid 와 tobe_uuid 를 비교 후, 같으면 tobe_uuid에만 통합진행, 다르면 asis, tobe 둘 다 진행
            if asis_uuid == tobe_uuid: 
                last_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==tobe_uuid).first()
                decide_priority(session, last_unified_record, seq, tobe_uuid)
            else:
                # asis_uuid 와 tobe_uuid 가 다른 경우,
                # asis_uuid 의 통합 고객이 새로 업데이트 되어지고, tobe_uuid 의 통합 고객도 새로 업데이트 되어져야함
                tobe_last_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==tobe_uuid).first()
                decide_priority(session, tobe_last_unified_record, seq, tobe_uuid)
                asis_last_unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid==asis_uuid).first()
                decide_priority(session, asis_last_unified_record, seq, tobe_uuid)


        # Delete
        elif task_dml_tp == 'DELETE':
            
            # Delete 시 asis_uuid로만 판단하고, 기존 통합 레코드 업데이트나 삭제되는 경우
            # Delete 되어진 고객이 link_customer 에서 남아있는지 검색
            last_link_records = session.query(gprtm.Link_customer).filter(gprtm.Link_customer.uuid==asis_uuid).all()
            unified_record = session.query(gprtm.Unify_customer).filter(gprtm.Unify_customer.uuid == asis_uuid).first()
            if len(last_link_records) > 0:
                # 남아있다면 통합 레코드 업데이트
                decide_priority(session, unified_record, seq, tobe_uuid)
            else :
                session.delete(unified_record)
                # 없으면 기존에 남아있던 통합 레코드 제거

        session.commit()

        return True
    
    except Exception as e:
        print(f'통합 과정 에러 발생 {e}')
        return False