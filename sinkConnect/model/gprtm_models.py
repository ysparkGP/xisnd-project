from sqlalchemy import MetaData, Column, Integer, String, VARCHAR, Date, TIMESTAMP, Sequence, JSON, BOOLEAN
from sqlalchemy.ext.declarative import declarative_base
import os
import sys

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
    error_tf = Column(BOOLEAN)
    error_retry_cnt = Column(Integer)
    register_dt = Column(TIMESTAMP)
    modify_dt = Column(TIMESTAMP)

    def __init__(self, seq, lv0_schema_nm, lv0_table_nm, dml_tp, cust_seq, asis_json, tobe_json, 
                 lv1_tf, lv1_external_tf, union_tf, link_tf, unified_tf, error_tf, error_retry_cnt, register_dt, modify_dt):
        self.seq = seq
        self.lv0_schema_nm = lv0_schema_nm
        self.lv0_table_nm = lv0_table_nm
        self.dml_tp = dml_tp
        self.cust_seq = cust_seq
        self.asis_json = asis_json
        self.tobe_json = tobe_json
        self.lv1_tf = lv1_tf
        self.lv1_external_tf = lv1_external_tf
        self.union_tf = union_tf
        self.link_tf = link_tf
        self.unified_tf = unified_tf
        self.error_tf = error_tf
        self.error_retry_cnt = error_retry_cnt
        self.register_dt = register_dt
        self.modify_dt = modify_dt

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
    cust_marketing = Column(VARCHAR())
    register_dt = Column(TIMESTAMP)
    modify_dt = Column(TIMESTAMP)

    def __init__(self,seq, channel_tp, cust_seq, cust_nm, cust_phone, cust_email, cust_addr, cust_addr_state, 
                 cust_addr_city, cust_birth_d, cust_register_dt, cust_modify_dt, cust_gender_tp, cust_marketing, register_dt, modify_dt):
        self.seq = seq
        self.channel_tp = channel_tp
        self.cust_seq = cust_seq
        self.cust_nm = cust_nm
        self.cust_phone = cust_phone
        self.cust_email = cust_email
        self.cust_addr = cust_addr
        self.cust_addr_state = cust_addr_state
        self.cust_addr_city = cust_addr_city
        self.cust_birth_d = cust_birth_d
        self.cust_register_dt = cust_register_dt
        self.cust_modify_dt = cust_modify_dt
        self.cust_gender_tp = cust_gender_tp
        self.cust_marketing = cust_marketing
        self.register_dt = register_dt
        self.modify_dt = modify_dt

class Link_customer(Base):

    __tablename__ = 'link_customer'

    seq = Column(Integer, primary_key=True)
    channel_tp = Column(VARCHAR())
    cust_seq = Column(VARCHAR())
    uuid = Column(VARCHAR())
    register_dt = Column(TIMESTAMP)
    modify_dt = Column(TIMESTAMP)

    def __init__(self,seq, channel_tp, cust_seq, uuid, register_dt, modify_dt):
        self.seq = seq
        self.channel_tp = channel_tp
        self.cust_seq = cust_seq
        self.uuid = uuid
        self.register_dt = register_dt
        self.modify_dt = modify_dt

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
    cust_marketing = Column(VARCHAR())
    register_dt = Column(TIMESTAMP)
    modify_dt = Column(TIMESTAMP)

    def __init__(self, uuid):
        self.seq = unify_sequence.next_value()
        self.uuid = uuid
        

    # def __init__(self, uuid, cust_nm, cust_phone, cust_email, cust_addr, cust_addr_state, cust_addr_city, 
    #              cust_birth_d, cust_register_dt, cust_modify_dt, cust_gender_tp, cust_marketing, register_dt, modify_dt):
    #     self.seq = unify_sequence.next_value()
    #     self.uuid = uuid
    #     self.cust_nm = cust_nm
    #     self.cust_phone = cust_phone
    #     self.cust_email = cust_email
    #     self.cust_addr = cust_addr
    #     self.cust_addr_state = cust_addr_state
    #     self.cust_addr_city = cust_addr_city
    #     self.cust_birth_d = cust_birth_d
    #     self.cust_register_dt = cust_register_dt
    #     self.cust_modify_dt = cust_modify_dt
    #     self.cust_gender_tp = cust_gender_tp
    #     self.cust_marketing = cust_marketing
    #     self.register_dt = register_dt
    #     self.modify_dt = modify_dt

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
    

    def __init__(self, seq, task_dml_tp, task_seq, channel_tp, cust_seq, asis_uuid, tobe_uuid, register_dt, modify_dt):
        self.seq = seq
        self.task_dml_tp = task_dml_tp
        self.task_seq = task_seq
        self.channel_tp = channel_tp
        self.cust_seq = cust_seq
        self.asis_uuid = asis_uuid
        self.tobe_uuid = tobe_uuid
        self.register_dt = register_dt
        self.modify_dt = modify_dt