import threading
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata
import ssl
import os
import json
import time
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from datetime import datetime
from lv1_external.external_manager import lv1_external
from function_manager import level_function
from json_create import json_create
from unify import unify_start
from model import gprtm_models as gprtm

ssl_cafile = os.environ['KAFKA_TRUSTED_CERT']
ssl_certfile = os.environ['KAFKA_CLIENT_CERT']
ssl_keyfile = os.environ['KAFKA_CLIENT_CERT_KEY']

os.system(f'echo -n "{ssl_cafile}" >> KAFKA_TRUSTED_CERT.pem')
os.system(f'echo -n "{ssl_certfile}" >> KAFKA_CLIENT_CERT.pem')
os.system(f'echo -n "{ssl_keyfile}" >> KAFKA_CLIENT_CERT_KEY.pem')

ssl_cafile = "KAFKA_TRUSTED_CERT.pem"
ssl_certfile = "KAFKA_CLIENT_CERT.pem"
ssl_keyfile = "KAFKA_CLIENT_CERT_KEY.pem"

db_name = os.environ['DB_NAME']
db_user = os.environ['DB_USER']
db_pass = os.environ['DB_PASS']
db_host = os.environ['DB_HOST']
db_port = os.environ['DB_PORT']

address_api_key = os.environ['ADDR_API_KEY']

next_num = 0
engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}', pool_size=15, max_overflow=20)
Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
topic = os.environ['TASK_TOPIC']

def sink_connect_setting():
    
    session = Session()

    bootstrap_server_string = os.environ['KAFKA_URL']
    bootstrap_servers = [server.replace("kafka+ssl://", "") for server in bootstrap_server_string.split(',')]

    topic_name = topic
    group_id = os.environ['GROUP_ID']

    ssl_context = ssl.create_default_context(cafile=ssl_cafile)
    ssl_context.load_cert_chain(certfile=ssl_certfile, keyfile=ssl_keyfile)
    ssl_context.check_hostname = False

    consumer = KafkaConsumer(
        topic_name,
        group_id=group_id,
        consumer_timeout_ms=-1,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        bootstrap_servers=bootstrap_servers,
        security_protocol='SSL',
        ssl_context=ssl_context,
        session_timeout_ms=70000,
        heartbeat_interval_ms=20000,
        max_poll_interval_ms=90000,
        max_poll_records=10,
        retry_backoff_ms=1000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x is not None else None
    )
    print('Connection Success')

    return session, consumer

def exception_log(message):
    print(str(message))

def job_log(schema_nm, table_nm, task_seq, job_func, message, t_name, processing_time):
    if job_func:
        print(f'[{t_name}]{task_seq} : {schema_nm}.{table_nm} {message} Success 소요시간 : {processing_time}')
    else:
        print(f'[{t_name}]{task_seq} : {schema_nm}.{table_nm} {message} Fail 소요시간 : {processing_time}')

def unified_job(session, schema_nm, table_nm, table_tp, channel_tp, job, task_seq, t_name, check, check_cnt ,start_time):
    task_record = session.query(gprtm.Task_manager).filter(gprtm.Task_manager.seq == task_seq).first()
    
    if task_record is None:
        raise Exception(f'task {task_seq} record 가 존재하지 않습니다.')

    if (getattr(task_record, job) != check) or (getattr(task_record, 'error_retry_cnt') is not None and check_cnt is not None and getattr(task_record, 'error_retry_cnt') > check_cnt):
        return
    
    tf = True
    if job == 'lv2_tf':
        if level_function(session,task_seq,2,schema_nm,table_nm):
            task_record.lv2_tf = True
            if task_record.error_tf:
                task_record.error_tf = False
                task_record.error_retry_cnt = 0
        
        tf = task_record.lv2_tf

        end_time = datetime.now()
        job_log(schema_nm, table_nm, task_seq, tf, 'Unfiy -> L2',t_name, end_time-start_time)

    elif job == 'unified_tf':
        
            if table_tp == 'I':
                if json_create(session):
                    result = session.query(gprtm.Link_customer_log).\
                                    filter(gprtm.Link_customer_log.task_seq == gprtm.Task_manager.seq,
                                        gprtm.Task_manager.seq == task_seq,
                                        gprtm.Link_customer_log.task_dml_tp != 'CHAIN').\
                                    first()
                    
                    if result is not None:
                        if unify_start(session, task_seq, task_record.dml_tp, result.asis_uuid, result.tobe_uuid, task_record.effected_uuid, task_record.changed_effected_uuid, table_tp):
                            task_record.unified_tf = True
                            if task_record.error_tf:
                                task_record.error_tf = False
                                task_record.error_retry_cnt = 0
                            tf = task_record.unified_tf
                            end_time = datetime.now()
                            job_log(schema_nm, table_nm, task_seq, tf, 'Link -> Unify',t_name, end_time-start_time)

                        else :
                            tf= False
                    else:
                        tf = False
                else:
                    tf = False
            
            else:
                result = session.query(gprtm.Link_customer).\
                                filter(gprtm.Marketing_customer.channel_tp == channel_tp,
                                       gprtm.Marketing_customer.cust_seq == task_record.cust_seq,
                                       gprtm.Marketing_customer.channel_tp == gprtm.Link_customer.channel_tp,
                                       gprtm.Marketing_customer.cust_seq == gprtm.Link_customer.cust_seq).\
                                first()
                
                if result is not None:
                    if unify_start(session, task_seq, task_record.dml_tp, None, result.uuid, None, None, table_tp):
                        task_record.unified_tf = True
                        if task_record.error_tf:
                            task_record.error_tf = False
                            task_record.error_retry_cnt = 0
                        tf = task_record.unified_tf
                        end_time = datetime.now()
                        job_log(schema_nm, table_nm, task_seq, tf, 'Link -> Unify',t_name, end_time-start_time)

                
                else:
                    unify_record = session.query(gprtm.Unify_customer).\
                                            filter(gprtm.Link_customer.channel_tp == channel_tp,
                                                gprtm.Link_customer.cust_seq == task_record.cust_seq,
                                                gprtm.Unify_customer.uuid == gprtm.Link_customer.uuid).\
                                            first()
                    
                    if unify_record is not None:
                        unify_record.cust_marketing_tf = None
                    
                    task_record.unified_tf = True
                    if task_record.error_tf:
                        task_record.error_tf = False
                        task_record.error_retry_cnt = 0
                    tf = task_record.unified_tf
                    end_time = datetime.now()
                    job_log(schema_nm, table_nm, task_seq, tf, 'Link -> Unify',t_name, end_time-start_time)

    elif job == 'link_tf':
        query = f' select gprtm.func_union_to_link({task_seq}); '
        result = session.execute(query)

        tf = result.all()[0][0]

        end_time = datetime.now()
        job_log(schema_nm, table_nm, task_seq, tf, 'Union -> Link',t_name, end_time-start_time)

    elif job == 'union_tf':
        query = f' select gprtm.func_lv1_to_union({task_seq}); '
        result = session.execute(query)
        
        tf = result.all()[0][0]

        end_time = datetime.now()
        job_log(schema_nm, table_nm, task_seq, tf, 'L1 -> Union',t_name, end_time-start_time)

    elif job == 'lv1_external_tf':
        tf = lv1_external(session,address_api_key,task_seq)

        end_time = datetime.now()
        job_log(schema_nm, table_nm, task_seq, tf, 'lv1 -> lv1_external',t_name, end_time-start_time)
        
    
    elif job == 'lv1_tf':
        tf = level_function(session,task_seq,1,schema_nm,table_nm)

        end_time = datetime.now()
        job_log(schema_nm, table_nm, task_seq, tf, 'L0 -> L1',t_name, end_time-start_time)
    
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
    cnt = 0
    for message in consumer:
        start_time = datetime.now()
        try:
            if message.value is None or message.value['payload']['after'] is None or message.value['payload']['op'] == 'd': 
                cnt+=1
                print(f'clean : {cnt} 시도')
                continue
            
            data = message.value['payload']['after']
            schema_nm = data['lv0_schema_nm']
            table_nm = data['lv0_table_nm']
            task_seq = data['seq']
            
            if message.value['payload']['op'] == 'c' or  message.value['payload']['op'] == 'u':
                check_query = f" select table_tp, channel_tp from gprtm.legacy_manager where lv0_schema_nm = '{schema_nm}' and lv0_table_nm = '{table_nm}' "
                check_result = session.execute(check_query).first()
                table_tp = check_result['table_tp']
                channel_tp = check_result['channel_tp']
                table_tp_dic = {'I': [0, 1, 2, 3, 4, 5], 'H': [0, 1, 4, 5], 'E': [0, 1, 5]}

                if data['error_retry_cnt'] != None:
                    if data['error_tf'] & ((data['error_retry_cnt']) >= 3): 
                        raise Exception(f'task_record {task_seq} 통합 과정 예외 발생(재시도 횟수 5회 초과)')
                
                for i in table_tp_dic[table_tp]:
                    if (data[job_list[i]] is not None) and (not data[job_list[i]]):
                        unified_job(session, schema_nm, table_nm, table_tp, channel_tp, job_list[i], task_seq, t_name, data[job_list[i]], data['error_retry_cnt'], start_time)
                        break

            else :
                continue
        except Exception as e:
            exception_log(e)

        finally:
            try:
                topic_partition = TopicPartition(topic, message.partition)
                meta = consumer.partitions_for_topic(topic)
                offsets = OffsetAndMetadata(message.offset+1, meta)
                options = {}
                options[topic_partition] = offsets
                session.commit()
                consumer.commit(offsets=options)
            except Exception as e:
                exception_log(e)
                time.sleep(5)
            
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