from sqlalchemy import MetaData, Column, Integer, String, VARCHAR, Date, TIMESTAMP, Sequence, JSON, BOOLEAN, func, DateTime, ARRAY
from sqlalchemy.ext.declarative import declarative_base
import os
import sys
import datetime

meta = MetaData(schema='gprtm')
Base = declarative_base(metadata=meta)
unify_sequence = Sequence('unify_seq')
class Task_manager(Base):

    __tablename__ = 'task_manager'

    seq = Column(Integer, primary_key=True)
    lv0_schema_nm = Column(VARCHAR())
    lv0_table_nm = Column(VARCHAR())
    dml_tp = Column(VARCHAR())
    cust_seq = Column(VARCHAR())
    asis_json = Column(JSON)
    tobe_json = Column(JSON)
    lv1_tf = Column(BOOLEAN)
    lv1_external_tf = Column(BOOLEAN)
    union_tf = Column(BOOLEAN)
    link_tf = Column(BOOLEAN)
    unified_tf = Column(BOOLEAN)
    lv2_tf = Column(BOOLEAN)
    error_tf = Column(BOOLEAN)
    error_retry_cnt = Column(Integer)
    register_dt = Column(TIMESTAMP)
    modify_dt = Column(TIMESTAMP)
    effected_uuid = Column(ARRAY(VARCHAR))
    changed_effected_uuid = Column(ARRAY(VARCHAR))

class Marketing_customer(Base):
    __tablename__ = 'marketing_customer'

    seq = Column(Integer, primary_key=True)
    channel_tp = Column(VARCHAR())
    cust_seq = Column(VARCHAR())
    cust_marketing_tp = Column(VARCHAR())
    cust_marketing_tf = Column(BOOLEAN)
    cust_modify_dt = Column(TIMESTAMP)
    register_dt = Column(TIMESTAMP)
    modify_dt = Column(TIMESTAMP)


class Union_customer(Base):

    __tablename__ = 'union_customer'

    seq = Column(Integer, primary_key=True)
    channel_tp = Column(VARCHAR())
    cust_seq = Column(VARCHAR())
    cust_nm = Column(VARCHAR())
    cust_phone = Column(VARCHAR())
    cust_email = Column(VARCHAR())
    cust_addr = Column(VARCHAR())
    cust_addr_state = Column(VARCHAR())
    cust_addr_city = Column(VARCHAR())
    cust_birth_d = Column(Date)
    cust_register_dt = Column(TIMESTAMP)
    cust_modify_dt = Column(TIMESTAMP)
    cust_gender_tp = Column(VARCHAR())
    register_dt = Column(TIMESTAMP)
    modify_dt = Column(TIMESTAMP)
    cust_hc_sfid = Column(VARCHAR())

class Link_customer(Base):

    __tablename__ = 'link_customer'

    seq = Column(Integer, primary_key=True)
    channel_tp = Column(VARCHAR())
    cust_seq = Column(VARCHAR())
    uuid = Column(VARCHAR())
    register_dt = Column(TIMESTAMP)
    modify_dt = Column(TIMESTAMP)


class Unify_customer(Base):

    __tablename__ = 'unify_customer'
    
    seq = Column(Integer, unify_sequence, primary_key=True)
    uuid = Column(VARCHAR())
    cust_nm = Column(VARCHAR())
    cust_phone = Column(VARCHAR())
    cust_email = Column(VARCHAR())
    cust_addr = Column(VARCHAR())
    cust_addr_state = Column(VARCHAR())
    cust_addr_city = Column(VARCHAR())
    cust_birth_d = Column(Date)
    cust_register_dt = Column(TIMESTAMP)
    cust_modify_dt = Column(TIMESTAMP)
    cust_gender_tp = Column(VARCHAR())
    cust_marketing_tf = Column(BOOLEAN)
    register_dt = Column(TIMESTAMP)
    modify_dt = Column(TIMESTAMP)
    cust_hc_sfid = Column(VARCHAR())

    def __init__(self, uuid):
        self.seq = unify_sequence.next_value()
        self.uuid = uuid
        self.register_dt = datetime.datetime.now()
        self.modify_dt = datetime.datetime.now()
        

class Link_customer_log(Base):

    __tablename__ = 'link_customer_log'
    
    seq = Column(Integer, primary_key=True)
    task_dml_tp = Column(VARCHAR())
    task_seq = Column(Integer)
    channel_tp = Column(VARCHAR())
    cust_seq = Column(VARCHAR())
    asis_uuid = Column(VARCHAR())
    tobe_uuid = Column(VARCHAR())
    register_dt = Column(TIMESTAMP)
    modify_dt = Column(TIMESTAMP)

class Test(Base):

    __tablename__ = 'test'

    seq = Column(Integer, primary_key=True)
    value = Column(VARCHAR())