import threading
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata
import ssl
import os
import json
import time
import psycopg2
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from dotenv import load_dotenv
from datetime import datetime
from lv1_external.external_manager import lv1_external
from function_manager import level_function
from json_create import json_create
from unify import unify_start
from model import gprtm_models as gprtm
load_dotenv()

db_name = os.environ['DB_NAME']
db_user = os.environ['DB_USER']
db_pass = os.environ['DB_PASS']
db_host = os.environ['DB_HOST']
db_port = os.environ['DB_PORT']

address_api_key = os.environ['ADDR_API_KEY']

log_dir = f'{os.path.dirname(os.path.realpath(__file__))}/log'
next_num = 0
engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}', pool_size=15, max_overflow=20)
Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
topic = 'brave_connector_78540.gprtm.task_manager'

def sink_connect_setting():
    
    session = Session()

    # Heroku Kafka 정보
    bootstrap_servers = ['ec2-13-114-158-85.ap-northeast-1.compute.amazonaws.com:9096',
                    'ec2-52-68-247-26.ap-northeast-1.compute.amazonaws.com:9096',
                    'ec2-54-64-83-210.ap-northeast-1.compute.amazonaws.com:9096']
    topic_name = topic
    group_id = 'python-cluster'
    dir = os.path.dirname(os.path.realpath(__file__))
    ssl_cafile = f'{dir}/ssl/KAFKA_TRUSTED_CERT.pem'
    ssl_certfile = f'{dir}/ssl/KAFKA_CLIENT_CERT.pem'
    ssl_keyfile = f'{dir}/ssl/KAFKA_CLIENT_CERT_KEY.pem'

    # SSL 인증 설정
    ssl_context = ssl.create_default_context(cafile=ssl_cafile)
    ssl_context.load_cert_chain(certfile=ssl_certfile, keyfile=ssl_keyfile)
    ssl_context.check_hostname = False

    # Kafka Consumer 설정
    consumer = KafkaConsumer(
        topic_name,
        group_id=group_id,
        consumer_timeout_ms=-1,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        bootstrap_servers=bootstrap_servers,
        security_protocol='SSL',
        ssl_context=ssl_context,
        session_timeout_ms=60000,
        heartbeat_interval_ms=20000,
        max_poll_interval_ms=50000,
        max_poll_records=10,
        retry_backoff_ms=1000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x is not None else None
    )

    print('Connection Success')

    return session, consumer

def exception_log(f, message):
    print(str(message))
    f.write(str(message))
    f.flush()

def job_log(f, schema_nm, table_nm, task_seq, job_func, message, t_name, processing_time):
    if job_func:
        f.write(f'{datetime.now()} [{t_name}]{task_seq} : {schema_nm}.{table_nm} {message} Success 소요시간 : {processing_time}\n')
        print(f'[{t_name}]{task_seq} : {schema_nm}.{table_nm} {message} Success 소요시간 : {processing_time}')
    else:
        f.write(f'{datetime.now()} [{t_name}]{task_seq} : {schema_nm}.{table_nm} {message} Fail 소요시간 : {processing_time}\n')
        print(f'[{t_name}]{task_seq} : {schema_nm}.{table_nm} {message} Fail 소요시간 : {processing_time}')
    f.flush()

# def error_update(session, seq, tf, error_tf, error_cnt):
#     if tf:
#         if error_tf == True:
#             update_error_query = f'update gprtm.task_manager set error_tf = false, error_retry_cnt = 0 where seq = \'{seq}\''
#             session.execute(update_error_query)

#     else:
#         query = f'update gprtm.task_manager set error_tf = true, error_retry_cnt = \'{error_cnt + 1}\' where seq = \'{seq}\''
#         session.execute(query)
    
#     session.commit()

def unified_job(f, session, schema_nm, table_nm, job, task_seq, t_name, check, check_cnt ,start_time):
    # print(f'{job} start!')
    # task_record 가 있을 경우에만 로직 진행
    task_record = session.query(gprtm.Task_manager).filter(gprtm.Task_manager.seq == task_seq).first()
    
    if task_record is None:
        raise Exception(f'task {task_seq} record 가 존재하지 않습니다.')
    
    # 중복된 메세지가 들어올 경우 처리
    # getattr(task_record,job) != data[job_list[i]] 로 현재 메시지 데이터(중복된 데이터)와 DB상에 존재하는 데이터의 상태가 상이할 경우
    # DB 상에 존재하는 error_retry_cnt 가 메시지의 error_retry_cnt 보다 더 클 경우(이미 실패한 메시지임)
    if (getattr(task_record, job) != check) or (getattr(task_record, 'error_retry_cnt') is not None and check_cnt is not None and getattr(task_record, 'error_retry_cnt') > check_cnt):
        # print(f'{getattr(task_record, job)}, {check}')
        return
    
    tf = True
    # L2
    if job == 'lv2_tf':
    # L1 정제 함수 호출
        if level_function(session,task_seq,2,schema_nm,table_nm):
            task_record.lv2_tf = True
            if task_record.error_tf:
                task_record.error_tf = False
                task_record.error_retry_cnt = 0
        
        tf = task_record.lv2_tf

        end_time = datetime.now()
        job_log(f, schema_nm, table_nm, task_seq, tf, 'Unfiy -> L2',t_name, end_time-start_time)

    # Unify job
    elif job == 'unified_tf':
        if json_create(session):
            # link_customer_log 호출 후 task_dml_tp, asis_uuid, tobe_uuid 할당
            result = session.query(gprtm.Link_customer_log).\
                            filter(gprtm.Link_customer_log.task_seq == gprtm.Task_manager.seq,
                                gprtm.Task_manager.seq == task_seq,
                                gprtm.Link_customer_log.task_dml_tp != 'CHAIN').\
                            first()
            
            # 통합함수 호출
            if unify_start(session, task_seq, task_record.dml_tp, result.asis_uuid, result.tobe_uuid, task_record.effected_uuid, task_record.changed_effected_uuid):
                task_record.unified_tf = True
                if task_record.error_tf:
                    task_record.error_tf = False
                    task_record.error_retry_cnt = 0
                    
            tf = task_record.unified_tf

            end_time = datetime.now()
            job_log(f, schema_nm, table_nm, task_seq, tf, 'Link -> Unify',t_name, end_time-start_time)

        else:
            tf = False
    
    # Link job
    elif job == 'link_tf':
        # Union -> Link 함수 호출
        # gprtm 스키마 func_union_to_link 함수 실행
        query = f' select gprtm.func_union_to_link({task_seq}); '
        result = session.execute(query)
        # print(result.all())

        tf = result.all()[0][0]

        end_time = datetime.now()
        job_log(f, schema_nm, table_nm, task_seq, tf, 'Union -> Link',t_name, end_time-start_time)
        
    
    # union job
    elif job == 'union_tf':
        # tf = False
        # L1 -> Union 함수 호출
        # gprtm 스키마 func_lv1_to_union 함수 실행
        query = f' select gprtm.func_lv1_to_union({task_seq}); '
        result = session.execute(query)
        
        tf = result.all()[0][0]

        end_time = datetime.now()
        job_log(f, schema_nm, table_nm, task_seq, tf, 'L1 -> Union',t_name, end_time-start_time)

    
    # lv1_external job
    elif job == 'lv1_external_tf':
        # L1 정제 함수 호출
        
        tf = lv1_external(session,address_api_key,task_seq)

        end_time = datetime.now()
        job_log(f, schema_nm, table_nm, task_seq, tf, 'lv1 -> lv1_external',t_name, end_time-start_time)
        
    
    elif job == 'lv1_tf':
        # lv0 to lv1 job
        
        tf = level_function(session,task_seq,1,schema_nm,table_nm)

        end_time = datetime.now()
        job_log(f, schema_nm, table_nm, task_seq, tf, 'L0 -> L1',t_name, end_time-start_time)
    
    if tf : 
        session.commit()
    else :
        setattr(task_record, job, False)
        task_record.error_tf = True
        task_record.error_retry_cnt = task_record.error_retry_cnt + 1 if task_record.error_retry_cnt is not None else 1
        session.commit()
        raise Exception(f'task_record {task_seq} 통합 과정 예외 발생({job} 단계)')

        
    
def sink_connect_start():
    global next_num
    job_list = ['lv1_tf', 'lv1_external_tf', 'union_tf', 'link_tf', 'unified_tf', 'lv2_tf']
    t_name = threading.current_thread().name
    session, consumer = sink_connect_setting()
    # cnt = 0
    # 메시지 수신
    with open(f'{log_dir}/{t_name}_log.txt', 'a', encoding='utf-8') as f:
        f.write(f'\n-------------- [{t_name}] START : {datetime.now()} --------------\n')
        for message in consumer:
            start_time = datetime.now()
            # print(message)
            # cnt+=1
            # print(f'{cnt} 시도')
            # topic_partition = TopicPartition(topic, message.partition)
            # meta = consumer.partitions_for_topic(topic)
            # offsets = OffsetAndMetadata(message.offset+1, meta)
            # options = {}
            # options[topic_partition] = offsets
            # consumer.commit(offsets=options)
            # continue
            try:
                if message.value is None or message.value['payload']['after'] is None: 
                    continue
                
                data = message.value['payload']['after']
                schema_nm = data['lv0_schema_nm']
                table_nm = data['lv0_table_nm']
                task_seq = data['seq']
                
                #insert, update
                if message.value['payload']['op'] == 'c' or  message.value['payload']['op'] == 'u':
                    # 테이블 작업이 Individual(I), Profile(P), History(H), ETC(E) 중 어디에서 일어나는지 확인 후 어떠한 로직을 태울 것인지 결정 
                    check_query = f" select table_tp from gprtm.legacy_manager where lv0_schema_nm = '{schema_nm}' and lv0_table_nm = '{table_nm}' "
                    table_tp = session.execute(check_query).first()['table_tp']
                    table_tp_dic = {'I': [0, 1, 2, 3, 4, 5], 'H': [0, 1, 5], 'E': [0, 1, 5]}
                    
                    # 에러 횟수가 5번 초과했는지부터 체크
                    if data['error_retry_cnt'] != None:
                        if data['error_tf'] & ((data['error_retry_cnt']) >= 3): 
                            raise Exception(f'task_record {task_seq} 통합 과정 예외 발생(재시도 횟수 5회 초과)')
                    
                    for i in table_tp_dic[table_tp]:
                        if (data[job_list[i]] is not None) and (not data[job_list[i]]):
                            unified_job(f, session, schema_nm, table_nm, job_list[i], task_seq, t_name, data[job_list[i]], data['error_retry_cnt'], start_time)
                            break

                #delete
                else :
                    # print(f"Delete : {message.value['payload']['before']}")
                    continue
            except Exception as e:
                exception_log(f, e)

            finally:
                try:
                    topic_partition = TopicPartition(topic, message.partition)
                    meta = consumer.partitions_for_topic(topic)
                    offsets = OffsetAndMetadata(message.offset+1, meta)
                    options = {}
                    options[topic_partition] = offsets
                    # print(offsets)
                    session.commit()
                    consumer.commit(offsets=options)
                except Exception as e:
                    exception_log(f,e)
                    # 파티션 리밸런싱 대기
                    time.sleep(5)
                
        # Consumer 종료
        consumer.close()

def make_threads(start, end):
    thread_li = []
    for i in range(start, end):
        thread_li.append(threading.Thread(target=sink_connect_start, name=f'Thread{i}'))
    return thread_li

def start_threads(thread_li):
    print(thread_li)
    for thread in thread_li:
        thread.start()
    

def main():
    global next_num
    next_num = 32
    thread_li = make_threads(0,32)
    start_threads(thread_li)

if __name__ == "__main__":
    main()