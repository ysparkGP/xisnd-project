from kafka import KafkaConsumer, KafkaProducer
import jpype
import jaydebeapi as jp
import ssl
import os
import psycopg2
from model import gprtm_models as gprtm
from dotenv import load_dotenv
import json
from json import dumps
import threading
from datetime import datetime
import copy
load_dotenv()

# Heroku Kafka 정보
bootstrap_servers = ['ec2-13-114-158-85.ap-northeast-1.compute.amazonaws.com:9096',
                'ec2-52-68-247-26.ap-northeast-1.compute.amazonaws.com:9096',
                'ec2-54-64-83-210.ap-northeast-1.compute.amazonaws.com:9096']
# topic_name = 'brave_connector_78540.gprtm.task_manager'
dir = os.path.dirname(os.path.realpath(__file__))
ssl_cafile = f'{dir}/ssl/KAFKA_TRUSTED_CERT.pem'
ssl_certfile = f'{dir}/ssl/KAFKA_CLIENT_CERT.pem'
ssl_keyfile = f'{dir}/ssl/KAFKA_CLIENT_CERT_KEY.pem'

# SSL 인증 설정
ssl_context = ssl.create_default_context(cafile=ssl_cafile)
ssl_context.load_cert_chain(certfile=ssl_certfile, keyfile=ssl_keyfile)
ssl_context.check_hostname = False
    
log_dir = f'{os.path.dirname(os.path.realpath(__file__))}/decrypt_log'
consumer_topics = ['DI.SNDIHS.TSM_CSINFO_', 'DI.SNDIHS.TSM_SUBSCR_', 'OM.IHS.TBM_CSINFO_']
producer_topics = ['ENC.SNDIHS.TSM_CSINFO_', 'ENC.SNDIHS.TSM_SUBSCR_', 'ENC.IHS.TBM_CSINFO_']

def job_log(f, table_nm, job_func, message, t_name):
    if job_func:
        f.write(f'{datetime.now()} [{t_name}] : {table_nm} {message} Success\n')
        print(f'{datetime.now()} [{t_name}] : {table_nm} {message} Success')
    else:
        f.write(f'[{t_name}] : {table_nm} {message} Fail\n')
        print(f'[{t_name}] : {table_nm} {message} Fail')
    f.flush()

def get_connect():

    conn_string = os.environ['ORACLE_DB_CONNECTION_STRING']
    userid_o = os.environ['ORACLE_DB_USER']
    passwd_o = os.environ['ORACLE_DB_PASS']

    # oracle 접근
    conn = jp.connect("oracle.jdbc.driver.OracleDriver", conn_string, [userid_o, passwd_o])
    cur_o = conn.cursor()

    print('DB Connection Success!')
    return cur_o

def decrypt_consumer_setting(topic_name):
    group_id = 'python-decrypt-consumer-cluster'
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
        max_poll_interval_ms=3000000,
        max_poll_records=10,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x is not None else None
    )
    print('Consumer Connections Success!')
    return consumer

def decrypt_procuder_setting(producer):
    group_id = 'python-decrypt-producer-cluster'
    producer = KafkaProducer(
        acks=0,
        compression_type='gzip',
        bootstrap_servers=bootstrap_servers,
        security_protocol='SSL',
        ssl_context=ssl_context,
        value_serializer=lambda x: dumps(x).encode('utf-8')
        # value_serializer=None
    )

    print('Producer Connection Success!')
    return producer

def decrypt_conumer_producer_start(order):
    cursor = get_connect()
    consumer = decrypt_consumer_setting(consumer_topics[order])
    producer = decrypt_procuder_setting(producer_topics[order])
    t_name = threading.current_thread().name
    with open(f'{log_dir}/{consumer_topics[order]}: {t_name}_log.txt', 'a', encoding='UTF-8') as f:
        for message in consumer:
            # UPSERT
            send_message = copy.deepcopy(message.value['payload'])
            cdc_table = send_message['source']['table']
            print(send_message)
            if send_message['op'] == 'c' or  send_message['op'] == 'u':
                if cdc_table == 'TSM_CSINFO#':
                    producer.send('decrypt_test_topic', send_message)
                    
                elif cdc_table == 'TSM_SUBSCR#':
                    producer.send('decrypt_test_topic', send_message)
                    print('전송완료')
                    
                else:
                    producer.send('decrypt_test_topic', send_message)
                    print('전송완료')
            
            elif send_message['op'] == 'r':
                # read?
                print('d')

            # DELETE
            else :
                if cdc_table == 'TSM_CSINFO#':
                    producer.send('decrypt_test_topic', send_message)
                    print('전송완료')
                elif cdc_table == 'TSM_SUBSCR#':
                    producer.send('decrypt_test_topic', send_message)
                    print('전송완료')
                else:
                    producer.send('decrypt_test_topic', send_message)
                    print('전송완료')

            consumer.commit()

        consumer.close()  
    return

def make_threads(start, end):
    thread_li = []
    for i in range(start, end):
        thread_li.append(threading.Thread(target=decrypt_conumer_producer_start, name=f'Decrypt_Thread{i}', args=(i,)))
    return thread_li

def start_threads(thread_li):
    print(thread_li)
    for thread in thread_li:
        thread.start()

def main():
    # oracle jdbc 파일 경로 및 class 경로 설정
    JDBC_Driver = f'{os.path.dirname(os.path.realpath(__file__))}/ojdbc8.jar'
    args = '-Djava.class.path=%s' % JDBC_Driver

    # java class path 설정
    jpype.startJVM(jpype.getDefaultJVMPath(), args)

    consumer_thread_li = make_threads(0,len(consumer_topics))
    start_threads(consumer_thread_li)

if __name__ == '__main__':
    main()