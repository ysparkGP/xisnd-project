from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.structs import OffsetAndMetadata
import jpype
import jaydebeapi as jp
import ssl
import os
from model import gprtm_models as gprtm
from dotenv import load_dotenv
import json
from json import dumps
import threading
from datetime import datetime
import copy
from pprint import pprint
import time
load_dotenv()


# Heroku Kafka 
bootstrap_servers = ['ec2-13-114-158-85.ap-northeast-1.compute.amazonaws.com:9096',
                'ec2-52-68-247-26.ap-northeast-1.compute.amazonaws.com:9096',
                'ec2-54-64-83-210.ap-northeast-1.compute.amazonaws.com:9096']
# topic_name = 'brave_connector_78540.gprtm.task_manager'
dir = os.path.dirname(os.path.realpath(__file__))
ssl_cafile = f'{dir}/ssl/KAFKA_TRUSTED_CERT.pem'
ssl_certfile = f'{dir}/ssl/KAFKA_CLIENT_CERT.pem'
ssl_keyfile = f'{dir}/ssl/KAFKA_CLIENT_CERT_KEY.pem'

# SSL 
ssl_context = ssl.create_default_context(cafile=ssl_cafile)
ssl_context.load_cert_chain(certfile=ssl_certfile, keyfile=ssl_keyfile)
ssl_context.check_hostname = False
    
log_dir = f'{os.path.dirname(os.path.realpath(__file__))}/decrypt_log'
producer_topics = ['DI.SNDIHS.TSM_CSINFO_', 'DI.SNDIHS.TSM_SUBSCR_']
# producer_topics = ['decrypt_test_topic', 'none_topic']
consumer_topics = ['ENC.SNDIHS.TSM_CSINFO_', 'ENC.SNDIHS.TSM_SUBSCR_']

def job_log(f, schema_nm, table_nm, task_seq, job_func, message, t_name):
    if job_func:
        f.write(f'{datetime.now()} [{t_name}]{task_seq} : {schema_nm}.{table_nm} {message} Success\n')
        print(f'{datetime.now()} [{t_name}]{task_seq} : {schema_nm}.{table_nm} {message} Success')
    else:
        f.write(f'[{t_name}]{task_seq} : {schema_nm}.{table_nm} {message} Fail\n')
        print(f'[{t_name}]{task_seq} : {schema_nm}.{table_nm} {message} Fail')
    f.flush()

def get_connect():

    # oracle jdbc 
    conn_string = "jdbc:oracle:thin:@165.243.39.43:1531:DBTST30"
    userid_o = "sndihs"
    passwd_o = "aaa"
    # host_p = "ec2-18-176-179-213.ap-northeast-1.compute.amazonaws.com"
    # userid_p = "task_worker"
    # passwd_p = "p6fe419be2c101ce8f1f51fa0394523a293cbdfb343fbe8e97776e03232b714df"
    # port_p = "5432"
    # db_p = "doq46gccfbqdp" 

    # oracle 
    conn = jp.connect("oracle.jdbc.driver.OracleDriver", conn_string, [userid_o, passwd_o])
    cur_o = conn.cursor()

    print('DB Connection Success!')
    return cur_o

def decrypt_consumer_setting(topic_name):
    group_id = 'python-decrypt-consumer-cluster'
    # Kafka Consumer 
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
        max_poll_interval_ms=30000,
        max_poll_records=10,
        retry_backoff_ms=1000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x is not None else None
    )
    print('Consumer Connections Success!')
    return consumer

def decrypt_procuder_setting():
    group_id = 'python-decrypt-producer-cluster'
    producer = KafkaProducer(
        acks=1,
        compression_type='gzip',
        bootstrap_servers=bootstrap_servers,
        security_protocol='SSL',
        ssl_context=ssl_context,
        # linger_ms=1000,
        value_serializer=lambda x: dumps(x).encode('utf-8') if x is not None else None,
        retries=3
        # value_serializer=None
    )

    print('Producer Connection Success!')
    return producer

def decrypt_conumer_producer_start(order):
    cursor = get_connect()
    consumer = decrypt_consumer_setting(consumer_topics[order])
    producer = decrypt_procuder_setting()
    t_name = threading.current_thread().name
    with open(f'{log_dir}/{t_name}_log.txt', 'a', encoding='UTF-8') as f:
        for message in consumer:
            try:
                # UPSERT
                send_message_key = copy.deepcopy(message.key)
                if message.value is None: 
                    response = producer.send(producer_topics[order], key=send_message_key)
                    continue
                send_message_value = copy.deepcopy(message.value)

                cdc_schema = send_message_value['payload']['source']['schema']
                cdc_table = send_message_value['payload']['source']['table']
                
                if send_message_value['payload']['op'] == 'c' or  send_message_value['payload']['op'] == 'u' or send_message_value['payload']['op'] == 'r':
                    # value['payload']['after']
                    decrypt_value(cdc_table, send_message_value, cursor, 'after')
                    producer.send(producer_topics[order], key = send_message_key, value = send_message_value)
                    
                    f.write(f'[{t_name}] : {cdc_schema}.{cdc_table} 복호화 완료\n')
                    
                # DELETE
                else :
                    # value['payload']['before']
                    # decrypt_value(cdc_table, send_message_value, cursor, 'before')
                    producer.send(producer_topics[order], key = send_message_key, value = send_message_value)
                   
              
            except Exception as e:
                print(f'예외상황발생 : {e}')

            finally:
                try:
                    topic_partition = TopicPartition(consumer_topics[order],message.partition)
                    meta = consumer.partitions_for_topic(consumer_topics[order])
                    offsets = OffsetAndMetadata(message.offset+1, meta)
                    options = {}
                    options[topic_partition] = offsets
                    print(offsets)
                    consumer.commit(offsets=options)
                except Exception as e:
                    print(e)
                
        consumer.close()  
    return

def decrypt_value(cdc_table, send_message_value, cursor, cover):
    # print(send_message_value)
    if cdc_table == 'TSM_CSINFO#':
        custid = send_message_value['payload'][cover]['CUSTID']
        # sql = f'select "CUSTID", "TRADEID", "NM", "FOREIGNERYN", "BIZPRSNDIVID", "RESNO", "CORPNO", "BIRTHDATE", "SOLARLUNARDIVID", "REPPRSNNM", "BIZNO", "BIZCOND", "BIZITEM", "REGZIPCD", "REGADDR1", "REGADDR2", "CONTACTZIPCD", "CONTACTADDR1", "CONTACTADDR2", "HOMETELNO", "HP", "CMPTELNO", "FAXNO", "SPOUSENM", "SPOUSEHP", "EMAIL", "REGUSERID", "REGDATE", "CHGUSERID", "CHGDATE", "HP2", "ADDR", "TAXBILLNM", "TAXBILLTELNO", "TAXBILLEMAIL", "TAXBILLDEPTNM", "TAXBILLHP", "ABOLITIONNO", "HPENDNO" from SNDIHS.TSM_CSINFO where CUSTID = {custid}'
        sql = f'select "CUSTID", "BIRTHDATE", "EMAIL", "REGADDR1" , "REGADDR2" , "CONTACTADDR1" , "CONTACTADDR2" from SNDIHS.TSM_CSINFO where CUSTID = {custid}'
        cursor.execute(sql)
        result = cursor.fetchone()

        if result is None : return
        send_message_value['payload'][cover]['BIRTHDATE'] = str(result[1]) if result[1] is not None else None
        send_message_value['payload'][cover]['EMAIL'] = str(result[2]) if result[2] is not None else None
        send_message_value['payload'][cover]['REGADDR1'] = str(result[3]) if result[3] is not None else None
        send_message_value['payload'][cover]['REGADDR2'] = str(result[4]) if result[4] is not None else None
        send_message_value['payload'][cover]['CONTACTADDR1'] = str(result[5]) if result[5] is not None else None
        send_message_value['payload'][cover]['CONTACTADDR2'] = str(result[6]) if result[6] is not None else None
        
    elif cdc_table == 'TSM_SUBSCR#':
        pjtcd = send_message_value['payload'][cover]['PJTCD']
        subscrseq = send_message_value['payload'][cover]['SUBSCRSEQ']
        # sql = f'select "PJTCD", "SUBSCRSEQ", "PRESEQ", "TYPE", "DONG", "HO", "NM", "RESNO", "ZIPCD", "ADDR", "TELNO", "BANK", "ACCTDIV", "ACCTNO", "RANK", "APPR", "PRIZE", "RECEIPTDATE", "LOWAPPRYN", "SUBSTRDATE", "GRADEAPPRYN", "HMLESSPERIOD", "FMCNT", "ACCTPERIOD", "HMCNT", "FMHMCNT", "GRADE", "DEGRADE", "TOTGRADE", "PRIZEDIV", "SMSYN", "SPECDIV", "PIZYN", "REGUSERID", "REGDATE", "CHGUSERID", "CHGDATE", "SUBPIZDIVID", "ROOMTYPECD", "BIRTHDATE", "ADDR2", "OLDHOUSEYN", "DISQRSN" from SNDIHS.TSM_SUBSCR where "PJTCD" = {pjtcd} and "SUBSCRSEQ" = {subscrseq}'
        sql = f'select "PJTCD", "SUBSCRSEQ", "TELNO", "ADDR", "RESNO" from SNDIHS.TSM_SUBSCR where "PJTCD" = \'{pjtcd}\' and "SUBSCRSEQ" = {subscrseq}'
        cursor.execute(sql)
        result = cursor.fetchone()

        if result is None: return
        send_message_value['payload'][cover]['TELNO'] = str(result[2]) if result[2] is not None else None
        send_message_value['payload'][cover]['ADDR'] = str(result[3]) if result[3] is not None else None
        send_message_value['payload'][cover]['RESNO'] = str(result[4]) if result[4] is not None else None

    elif cdc_table == 'TBM_CSINFO#':
        # custid = send_message_value['payload'][cover]['CUSTID']
        # sql = f'select "CUSTID", "TRADEID", "NM", "FOREIGNERYN", "BIZPRSNDIVID", "RESNO", "CORPNO", "BIRTHDATE", "SOLARLUNARDIVID", "REPPRSNNM", "BIZNO", "BIZCOND", "BIZITEM", "REGZIPCD", "REGADDR1", "REGADDR2", "CONTACTZIPCD", "CONTACTADDR1", "CONTACTADDR2", "HOMETELNO", "HP", "HP2", "CMPTELNO", "FAXNO", "GNRTELNO", "RMK", "EMAIL", "ABOLITIONNO", "REGUSERID", "REGDATE", "CHGUSERID", "CHGDATE" from IHS.TBM_CSINFO where "CUSTID" = {custid}' 
        # cursor.execute(sql)
        # result = cursor.fetchone()
        pass
    
    else : pass
    # print(send_message_value)
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
    JDBC_Driver = 'C:/oracle/ojdbc8.jar'
    args = '-Djava.class.path=%s' % JDBC_Driver

    # java class path
    jpype.startJVM(jpype.getDefaultJVMPath(), args)

    consumer_thread_li = make_threads(0,len(consumer_topics))
    start_threads(consumer_thread_li)

if __name__ == '__main__':
    main()