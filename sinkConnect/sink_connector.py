from kafka import KafkaConsumer
import ssl
import os
import json
import psycopg2
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from datetime import datetime
from jsonCreate import json_create
from unify import unify_start
from model import gprtm_models as gprtm
load_dotenv()

db_name = os.environ['DB_NAME']
db_user = os.environ['DB_USER']
db_pass = os.environ['DB_PASS']
db_host = os.environ['DB_HOST']
db_port = os.environ['DB_PORT']

log_dir = f'{os.path.dirname(os.path.realpath(__file__))}/log'

engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')
Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session = Session()

# Heroku Kafka 정보
bootstrap_servers = ['ec2-13-114-158-85.ap-northeast-1.compute.amazonaws.com:9096',
                'ec2-52-68-247-26.ap-northeast-1.compute.amazonaws.com:9096',
                'ec2-54-64-83-210.ap-northeast-1.compute.amazonaws.com:9096']
topic_name = 'exuberant_connector_81605.gprtm.task_manager'
# group_id = 'mississippi-85469.test-ys-group'
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
    # group_id=group_id,
    consumer_timeout_ms=-1,
    enable_auto_commit=True,
    # auto_offset_reset='earliest',
    bootstrap_servers=bootstrap_servers,
    security_protocol='SSL',
    ssl_context=ssl_context,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x is not None else None
)

print('Connection Success');

# 메시지 수신
with open(f'{log_dir}/log.txt', 'w', encoding='utf-8') as f:
    for message in consumer:
        if message.value is None: continue

        data = message.value['payload']['after']
        seq = data['seq']
        schema_nm = data['lv0_schema_nm']
        table_nm = data['lv0_table_nm']
        
        #insert, update
        if message.value['payload']['op'] == 'c' or  message.value['payload']['op'] == 'u':
            
            if data['error_tf'] : continue

            if data['lv1_tf']:
                # if data['lv1_external_tf']:
                    if data['union_tf']:
                        if data['link_tf']:
                            if data['unified_tf']:
                                continue
                            else:
                                if json_create(session):
                                    # link_customer_log 호출 후 task_dml_tp, asis_uuid, tobe_uuid 할당
                                    result = session.query(gprtm.Link_customer_log).\
                                                    filter(gprtm.Link_customer_log.task_seq == gprtm.Task_manager.seq,
                                                        gprtm.Task_manager.seq == seq).\
                                                    first()
                                    # 통합 함수 호출
                                    if unify_start(session, seq, result.task_dml_tp, result.asis_uuid, result.tobe_uuid):
                                        # 성공 로그
                                        f.write(f'{schema_nm}.{table_nm} Link -> Unify Success\n')
                                        print(f'{schema_nm}.{table_nm} Link -> Unify 성공')
                                        # task_manager unify_tf = True
                                    else:
                                        f.write(f'{schema_nm}.{table_nm} Link -> Unify Fail\n')
                                        print(f'{schema_nm}.{table_nm} Link -> Unify 실패')
                                        # task_manager unify_tf = False
                                else:
                                    continue
                                
                        else:
                            # Union -> Link 함수 호출
                            query = f' select gprtm.func_union_to_link({seq}); '
                            result = session.execute(query)
                            # print(result.all())

                            tf = result.all()[0][0]
                            if tf:
                                # 성공 로그
                                f.write(f'{schema_nm}.{table_nm} Union -> Link Success\n')
                                print(f'{schema_nm}.{table_nm} Union -> Link 성공')
                            else:
                                # 실패 로그
                                f.write(f'{schema_nm}.{table_nm} Union -> Link Fail\n')
                                print(f'{schema_nm}.{table_nm} Union -> Link 실패')

                    else:
                        # L1 -> Union 함수 호출
                        query = f' select gprtm.func_lv1_to_union({seq}); '
                        result = session.execute(query)
                        
                        tf = result.all()[0][0]
                        if tf:
                            # 성공 로그
                            f.write(f'{schema_nm}.{table_nm} L1 -> Union Success\n')
                            print(f'{schema_nm}.{table_nm} L1 -> Union 성공')
                        else:
                            # 실패 로그
                            f.write(f'{schema_nm}.{table_nm} L1 -> Union Fail\n')
                            print(f'{schema_nm}.{table_nm} L1 -> Union 실패')

                # else:
                #     # L1 정제 함수 호출
                #     query = ''
                #     session.execute(query)

            else:
                
                query = f' select "{schema_nm}"."func_{table_nm}"({seq}); '
                # print(query)

                result = session.execute(query)
                # [(결과값,)]
                tf = result.all()[0][0]
                if tf:
                    # 성공 로그
                    f.write(f'{schema_nm}.{table_nm} L0 -> L1 Success\n')
                    print(f'{schema_nm}.{table_nm} L0 -> L1 성공')
                else:
                    # 실패 로그
                    f.write(f'{schema_nm}.{table_nm} L0 -> L1 Fail\n')
                    print(f'{schema_nm}.{table_nm} L0 -> L1 실패')
                # L0 -> L1 함수 호출
            
            session.commit()

        #delete
        else :
            continue
            # print(f"Delete : {message.value['payload']['before']}")
            # print()
            # data = message.value['payload']['before']
        f.flush()
    # Consumer 종료
consumer.close()