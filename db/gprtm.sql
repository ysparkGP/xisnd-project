-- DROP SCHEMA gprtm;

CREATE SCHEMA gprtm AUTHORIZATION udaivbbgsjgrg9;

-- DROP SEQUENCE gprtm.function_manager_seq_seq;

CREATE SEQUENCE gprtm.function_manager_seq_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE gprtm.legacy_manager_seq_seq;

CREATE SEQUENCE gprtm.legacy_manager_seq_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE gprtm.link_customer_seq_seq;

CREATE SEQUENCE gprtm.link_customer_seq_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE gprtm.marketing_customer_seq_seq;

CREATE SEQUENCE gprtm.marketing_customer_seq_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE gprtm.partition_manager_seq_seq;

CREATE SEQUENCE gprtm.partition_manager_seq_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE gprtm.rule_manager_seq_seq;

CREATE SEQUENCE gprtm.rule_manager_seq_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE gprtm.task_manager_seq_seq;

CREATE SEQUENCE gprtm.task_manager_seq_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE gprtm.unify_customer_seq_seq;

CREATE SEQUENCE gprtm.unify_customer_seq_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE gprtm.union_customer_seq_seq;

CREATE SEQUENCE gprtm.union_customer_seq_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;-- gprtm."_change_union_link_table" definition

-- Drop table

-- DROP TABLE gprtm."_change_union_link_table";

CREATE TABLE gprtm."_change_union_link_table" (
	uuid varchar NULL,
	channel_tp varchar NULL,
	cust_seq varchar NULL,
	cust_nm varchar NULL,
	cust_phone varchar NULL,
	cust_email varchar NULL,
	register_dt timestamp NULL,
	changed_uuid varchar NULL
);


-- gprtm."_temp_union_link_table" definition

-- Drop table

-- DROP TABLE gprtm."_temp_union_link_table";

CREATE TABLE gprtm."_temp_union_link_table" (
	uuid varchar NULL,
	channel_tp varchar NULL,
	cust_seq varchar NULL,
	cust_nm varchar NULL,
	cust_phone varchar NULL,
	cust_email varchar NULL,
	register_dt timestamp NULL,
	cnt int4 NULL
);


-- gprtm.function_manager definition

-- Drop table

-- DROP TABLE gprtm.function_manager;

CREATE TABLE gprtm.function_manager (
	seq serial4 NOT NULL,
	lv0_schema_nm varchar NULL,
	lv0_table_nm varchar NULL,
	level_num float4 NULL,
	function_nm varchar NULL,
	priority_num int4 NULL,
	register_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	modify_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	enable_tf bool NULL DEFAULT true,
	CONSTRAINT function_manager_pk PRIMARY KEY (seq),
	CONSTRAINT function_manager_un UNIQUE (lv0_schema_nm, lv0_table_nm, level_num, function_nm, priority_num)
);

-- Table Triggers

create trigger trig_update_modifiy_dt before
update
    on
    gprtm.function_manager for each row execute function gprtm.func_update_modifiy_dt();


-- gprtm.legacy_manager definition

-- Drop table

-- DROP TABLE gprtm.legacy_manager;

CREATE TABLE gprtm.legacy_manager (
	seq serial4 NOT NULL,
	lv0_schema_nm varchar NULL,
	lv0_table_nm varchar NULL,
	lv1_schema_nm varchar NULL,
	lv1_table_nm varchar NULL,
	channel_tp varchar NULL,
	table_tp varchar NULL DEFAULT false,
	register_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	modify_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	CONSTRAINT legacy_manager_pk PRIMARY KEY (seq)
);

-- Table Triggers

create trigger trig_update_modifiy_dt before
update
    on
    gprtm.legacy_manager for each row execute function gprtm.func_update_modifiy_dt();


-- gprtm.link_customer definition

-- Drop table

-- DROP TABLE gprtm.link_customer;

CREATE TABLE gprtm.link_customer (
	seq serial4 NOT NULL,
	channel_tp varchar NULL,
	cust_seq varchar NULL,
	uuid varchar NULL,
	register_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	modify_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	CONSTRAINT link_customer_pk PRIMARY KEY (seq),
	CONSTRAINT link_customer_un UNIQUE (channel_tp, cust_seq)
);
CREATE INDEX link_customer_uuid_idx ON gprtm.link_customer USING btree (uuid);

-- Table Triggers

create trigger trig_update_modifiy_dt before
update
    on
    gprtm.link_customer for each row execute function gprtm.func_update_modifiy_dt();


-- gprtm.link_customer_log definition

-- Drop table

-- DROP TABLE gprtm.link_customer_log;

CREATE TABLE gprtm.link_customer_log (
	seq int4 NOT NULL,
	task_dml_tp varchar NULL,
	task_seq int4 NULL,
	channel_tp varchar NULL,
	cust_seq varchar NULL,
	asis_uuid varchar NULL,
	tobe_uuid varchar NULL,
	register_dt timestamp NULL,
	modify_dt timestamp NULL
)
PARTITION BY RANGE (modify_dt);
CREATE INDEX link_customer_log_task_seq_idx ON ONLY gprtm.link_customer_log USING btree (task_seq);


-- gprtm.marketing_customer definition

-- Drop table

-- DROP TABLE gprtm.marketing_customer;

CREATE TABLE gprtm.marketing_customer (
	seq serial4 NOT NULL,
	channel_tp varchar NULL,
	cust_seq varchar NULL,
	cust_marketing_tp varchar NULL,
	cust_marketing_tf bool NULL,
	cust_modify_dt timestamp NULL,
	register_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	modify_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	CONSTRAINT marketing_customer_pk PRIMARY KEY (seq),
	CONSTRAINT marketing_customer_un UNIQUE (channel_tp, cust_seq, cust_marketing_tp)
);

-- Table Triggers

create trigger trig_data_to_log after
insert
    or
delete
    or
update
    on
    gprtm.marketing_customer for each row execute function gprtm.func_marketing_to_log();
create trigger trig_update_modifiy_dt before
update
    on
    gprtm.marketing_customer for each row execute function gprtm.func_update_modifiy_dt();


-- gprtm.marketing_customer_log definition

-- Drop table

-- DROP TABLE gprtm.marketing_customer_log;

CREATE TABLE gprtm.marketing_customer_log (
	seq int4 NULL,
	dml_tp varchar NULL,
	channel_tp varchar NULL,
	cust_seq varchar NULL,
	cust_marketing_tp varchar NULL,
	cust_marketing_tf bool NULL,
	cust_modify_dt timestamp NULL,
	register_dt timestamp NULL,
	modify_dt timestamp NULL
)
PARTITION BY RANGE (modify_dt);


-- gprtm.partition_manager definition

-- Drop table

-- DROP TABLE gprtm.partition_manager;

CREATE TABLE gprtm.partition_manager (
	seq serial4 NOT NULL,
	schema_nm varchar NOT NULL,
	table_nm varchar NULL,
	partition_tf bool NULL DEFAULT false,
	store_d_num int4 NOT NULL DEFAULT 0,
	register_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	modify_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	CONSTRAINT partition_manager_pk PRIMARY KEY (seq),
	CONSTRAINT partition_manager_un UNIQUE (table_nm, schema_nm)
);

-- Table Triggers

create trigger trig_update_modifiy_dt before
update
    on
    gprtm.partition_manager for each row execute function gprtm.func_update_modifiy_dt();


-- gprtm.rule_manager definition

-- Drop table

-- DROP TABLE gprtm.rule_manager;

CREATE TABLE gprtm.rule_manager (
	seq serial4 NOT NULL,
	rule_json json NULL,
	default_json json NULL,
	condition_json json NULL,
	register_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	modify_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	CONSTRAINT rule_manager_pk PRIMARY KEY (seq)
);

-- Table Triggers

create trigger trig_data_to_log after
insert
    or
delete
    or
update
    on
    gprtm.rule_manager for each row execute function gprtm.func_rule_to_log();
create trigger trig_update_modifiy_dt before
update
    on
    gprtm.rule_manager for each row execute function gprtm.func_update_modifiy_dt();


-- gprtm.rule_manager_log definition

-- Drop table

-- DROP TABLE gprtm.rule_manager_log;

CREATE TABLE gprtm.rule_manager_log (
	seq int4 NULL,
	task_dml_tp varchar NULL,
	rule_json json NULL,
	default_json json NULL,
	condition_json json NULL,
	register_dt timestamp NULL,
	modify_dt timestamp NULL
)
PARTITION BY RANGE (modify_dt);


-- gprtm.task_manager definition

-- Drop table

-- DROP TABLE gprtm.task_manager;

CREATE TABLE gprtm.task_manager (
	seq serial4 NOT NULL,
	lv0_schema_nm varchar NOT NULL,
	lv0_table_nm varchar NOT NULL,
	dml_tp varchar NOT NULL,
	cust_seq varchar NULL,
	asis_json json NULL,
	tobe_json json NULL,
	lv1_tf bool NULL DEFAULT false,
	lv1_external_tf bool NULL DEFAULT false,
	union_tf bool NULL,
	link_tf bool NULL,
	unified_tf bool NULL,
	lv2_tf bool NULL DEFAULT false,
	error_tf bool NULL DEFAULT false,
	error_retry_cnt int4 NULL,
	register_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	modify_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	effected_uuid _varchar NULL,
	changed_effected_uuid _varchar NULL,
	CONSTRAINT task_manager_pk PRIMARY KEY (seq)
);

-- Table Triggers

create trigger trig_update_modifiy_dt before
update
    on
    gprtm.task_manager for each row execute function gprtm.func_update_modifiy_dt();
create trigger trig_data_to_log after
insert
    or
update
    on
    gprtm.task_manager for each row execute function gprtm.func_task_to_log();


-- gprtm.task_manager_log definition

-- Drop table

-- DROP TABLE gprtm.task_manager_log;

CREATE TABLE gprtm.task_manager_log (
	seq int4 NOT NULL,
	task_dml_tp varchar NOT NULL,
	lv0_schema_nm varchar NOT NULL,
	lv0_table_nm varchar NOT NULL,
	dml_tp varchar NOT NULL,
	cust_seq varchar NULL,
	asis_json json NULL,
	tobe_json json NULL,
	lv1_tf bool NULL DEFAULT false,
	lv1_external_tf bool NULL DEFAULT false,
	union_tf bool NULL,
	link_tf bool NULL,
	unified_tf bool NULL,
	lv2_tf bool NULL DEFAULT false,
	error_tf bool NULL DEFAULT false,
	error_retry_cnt int4 NULL,
	register_dt timestamp NULL,
	modify_dt timestamp NULL,
	effected_uuid _varchar NULL,
	changed_effected_uuid _varchar NULL
)
PARTITION BY RANGE (modify_dt);


-- gprtm.test definition

-- Drop table

-- DROP TABLE gprtm.test;

CREATE TABLE gprtm.test (
	seq int4 NOT NULL,
	cust_nm varchar(250) NULL,
	phone varchar NULL,
	reg_date timestamp NULL DEFAULT now(),
	CONSTRAINT test_pkey PRIMARY KEY (seq),
	CONSTRAINT test_un UNIQUE (phone)
);

-- Table Triggers

create trigger test_trigger before
insert
    or
update
    on
    gprtm.test for each row execute function gprtm.test_trigger_function();


-- gprtm.test2 definition

-- Drop table

-- DROP TABLE gprtm.test2;

CREATE TABLE gprtm.test2 (
	seq int4 NOT NULL,
	value text NULL,
	phone text NULL,
	reg_date timestamp NULL,
	CONSTRAINT test2_pk PRIMARY KEY (seq)
);


-- gprtm.unify_customer definition

-- Drop table

-- DROP TABLE gprtm.unify_customer;

CREATE TABLE gprtm.unify_customer (
	seq serial4 NOT NULL,
	uuid varchar NOT NULL,
	cust_nm varchar NULL,
	cust_phone varchar NULL,
	cust_email varchar NULL,
	cust_addr varchar NULL,
	cust_addr_state varchar NULL,
	cust_addr_city varchar NULL,
	cust_birth_d date NULL,
	cust_register_dt timestamp NULL,
	cust_modify_dt timestamp NULL,
	cust_gender_tp varchar NULL,
	register_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	modify_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP),
	cust_hc_sfid varchar NULL,
	cust_marketing_tf bool NULL,
	CONSTRAINT unify_customer_pk PRIMARY KEY (seq),
	CONSTRAINT unify_customer_un UNIQUE (uuid)
);

-- Table Triggers

create trigger trig_update_modifiy_dt before
update
    on
    gprtm.unify_customer for each row execute function gprtm.func_update_modifiy_dt();


-- gprtm.union_customer definition

-- Drop table

-- DROP TABLE gprtm.union_customer;

CREATE TABLE gprtm.union_customer (
	seq serial4 NOT NULL,
	channel_tp varchar NULL, -- 채널 타입
	cust_seq varchar NULL, -- 고객 시퀀스
	cust_nm varchar NULL, -- 고객 이름
	cust_phone varchar NULL, -- 고객 전화번호
	cust_email varchar NULL, -- 고객 이메일
	cust_addr varchar NULL, -- 고객 주소
	cust_addr_state varchar NULL, -- 고객 주소 - 시
	cust_addr_city varchar NULL, -- 고객 주소 - 구
	cust_birth_d date NULL, -- 고객 생년월일
	cust_register_dt timestamp NULL, -- 고객 등록일
	cust_modify_dt timestamp NULL, -- 고객 수정일
	cust_gender_tp varchar NULL, -- 고객 성별 타입
	register_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP), -- 레코드 등록일
	modify_dt timestamp NULL DEFAULT timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP), -- 레코드 수정일
	cust_hc_sfid varchar NULL,
	CONSTRAINT union_customer_pk PRIMARY KEY (seq),
	CONSTRAINT union_customer_un UNIQUE (channel_tp, cust_seq)
);

-- Column comments

COMMENT ON COLUMN gprtm.union_customer.channel_tp IS '채널 타입';
COMMENT ON COLUMN gprtm.union_customer.cust_seq IS '고객 시퀀스';
COMMENT ON COLUMN gprtm.union_customer.cust_nm IS '고객 이름';
COMMENT ON COLUMN gprtm.union_customer.cust_phone IS '고객 전화번호';
COMMENT ON COLUMN gprtm.union_customer.cust_email IS '고객 이메일';
COMMENT ON COLUMN gprtm.union_customer.cust_addr IS '고객 주소';
COMMENT ON COLUMN gprtm.union_customer.cust_addr_state IS '고객 주소 - 시';
COMMENT ON COLUMN gprtm.union_customer.cust_addr_city IS '고객 주소 - 구';
COMMENT ON COLUMN gprtm.union_customer.cust_birth_d IS '고객 생년월일';
COMMENT ON COLUMN gprtm.union_customer.cust_register_dt IS '고객 등록일';
COMMENT ON COLUMN gprtm.union_customer.cust_modify_dt IS '고객 수정일';
COMMENT ON COLUMN gprtm.union_customer.cust_gender_tp IS '고객 성별 타입';
COMMENT ON COLUMN gprtm.union_customer.register_dt IS '레코드 등록일';
COMMENT ON COLUMN gprtm.union_customer.modify_dt IS '레코드 수정일';

-- Table Triggers

create trigger trig_update_modifiy_dt before
update
    on
    gprtm.union_customer for each row execute function gprtm.func_update_modifiy_dt();


-- gprtm.link_customer_log_20230917 definition

CREATE TABLE gprtm.link_customer_log_20230917 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-09-17 00:00:00') TO ('2023-09-18 00:00:00');


-- gprtm.link_customer_log_20230918 definition

CREATE TABLE gprtm.link_customer_log_20230918 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-09-18 00:00:00') TO ('2023-09-19 00:00:00');


-- gprtm.link_customer_log_20230919 definition

CREATE TABLE gprtm.link_customer_log_20230919 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-09-19 00:00:00') TO ('2023-09-20 00:00:00');


-- gprtm.link_customer_log_20230920 definition

CREATE TABLE gprtm.link_customer_log_20230920 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-09-20 00:00:00') TO ('2023-09-21 00:00:00');


-- gprtm.link_customer_log_20230921 definition

CREATE TABLE gprtm.link_customer_log_20230921 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-09-21 00:00:00') TO ('2023-09-22 00:00:00');


-- gprtm.link_customer_log_20230922 definition

CREATE TABLE gprtm.link_customer_log_20230922 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-09-22 00:00:00') TO ('2023-09-23 00:00:00');


-- gprtm.link_customer_log_20230923 definition

CREATE TABLE gprtm.link_customer_log_20230923 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-09-23 00:00:00') TO ('2023-09-24 00:00:00');


-- gprtm.link_customer_log_20230924 definition

CREATE TABLE gprtm.link_customer_log_20230924 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-09-24 00:00:00') TO ('2023-09-25 00:00:00');


-- gprtm.link_customer_log_20230925 definition

CREATE TABLE gprtm.link_customer_log_20230925 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-09-25 00:00:00') TO ('2023-09-26 00:00:00');


-- gprtm.link_customer_log_20230926 definition

CREATE TABLE gprtm.link_customer_log_20230926 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-09-26 00:00:00') TO ('2023-09-27 00:00:00');


-- gprtm.link_customer_log_20230927 definition

CREATE TABLE gprtm.link_customer_log_20230927 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-09-27 00:00:00') TO ('2023-09-28 00:00:00');


-- gprtm.link_customer_log_20230928 definition

CREATE TABLE gprtm.link_customer_log_20230928 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-09-28 00:00:00') TO ('2023-09-29 00:00:00');


-- gprtm.link_customer_log_20230929 definition

CREATE TABLE gprtm.link_customer_log_20230929 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-09-29 00:00:00') TO ('2023-09-30 00:00:00');


-- gprtm.link_customer_log_20230930 definition

CREATE TABLE gprtm.link_customer_log_20230930 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-09-30 00:00:00') TO ('2023-10-01 00:00:00');


-- gprtm.link_customer_log_20231001 definition

CREATE TABLE gprtm.link_customer_log_20231001 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-01 00:00:00') TO ('2023-10-02 00:00:00');


-- gprtm.link_customer_log_20231002 definition

CREATE TABLE gprtm.link_customer_log_20231002 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-02 00:00:00') TO ('2023-10-03 00:00:00');


-- gprtm.link_customer_log_20231003 definition

CREATE TABLE gprtm.link_customer_log_20231003 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-03 00:00:00') TO ('2023-10-04 00:00:00');


-- gprtm.link_customer_log_20231004 definition

CREATE TABLE gprtm.link_customer_log_20231004 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-04 00:00:00') TO ('2023-10-05 00:00:00');


-- gprtm.link_customer_log_20231005 definition

CREATE TABLE gprtm.link_customer_log_20231005 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-05 00:00:00') TO ('2023-10-06 00:00:00');


-- gprtm.link_customer_log_20231006 definition

CREATE TABLE gprtm.link_customer_log_20231006 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-06 00:00:00') TO ('2023-10-07 00:00:00');


-- gprtm.link_customer_log_20231007 definition

CREATE TABLE gprtm.link_customer_log_20231007 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-07 00:00:00') TO ('2023-10-08 00:00:00');


-- gprtm.link_customer_log_20231008 definition

CREATE TABLE gprtm.link_customer_log_20231008 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-08 00:00:00') TO ('2023-10-09 00:00:00');


-- gprtm.link_customer_log_20231009 definition

CREATE TABLE gprtm.link_customer_log_20231009 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-09 00:00:00') TO ('2023-10-10 00:00:00');


-- gprtm.link_customer_log_20231010 definition

CREATE TABLE gprtm.link_customer_log_20231010 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-10 00:00:00') TO ('2023-10-11 00:00:00');


-- gprtm.link_customer_log_20231011 definition

CREATE TABLE gprtm.link_customer_log_20231011 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-11 00:00:00') TO ('2023-10-12 00:00:00');


-- gprtm.link_customer_log_20231012 definition

CREATE TABLE gprtm.link_customer_log_20231012 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-12 00:00:00') TO ('2023-10-13 00:00:00');


-- gprtm.link_customer_log_20231013 definition

CREATE TABLE gprtm.link_customer_log_20231013 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-13 00:00:00') TO ('2023-10-14 00:00:00');


-- gprtm.link_customer_log_20231014 definition

CREATE TABLE gprtm.link_customer_log_20231014 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-14 00:00:00') TO ('2023-10-15 00:00:00');


-- gprtm.link_customer_log_20231015 definition

CREATE TABLE gprtm.link_customer_log_20231015 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-15 00:00:00') TO ('2023-10-16 00:00:00');


-- gprtm.link_customer_log_20231016 definition

CREATE TABLE gprtm.link_customer_log_20231016 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-16 00:00:00') TO ('2023-10-17 00:00:00');


-- gprtm.link_customer_log_20231017 definition

CREATE TABLE gprtm.link_customer_log_20231017 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-17 00:00:00') TO ('2023-10-18 00:00:00');


-- gprtm.link_customer_log_20231018 definition

CREATE TABLE gprtm.link_customer_log_20231018 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-18 00:00:00') TO ('2023-10-19 00:00:00');


-- gprtm.link_customer_log_20231019 definition

CREATE TABLE gprtm.link_customer_log_20231019 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-19 00:00:00') TO ('2023-10-20 00:00:00');


-- gprtm.link_customer_log_20231020 definition

CREATE TABLE gprtm.link_customer_log_20231020 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-20 00:00:00') TO ('2023-10-21 00:00:00');


-- gprtm.link_customer_log_20231021 definition

CREATE TABLE gprtm.link_customer_log_20231021 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-21 00:00:00') TO ('2023-10-22 00:00:00');


-- gprtm.link_customer_log_20231022 definition

CREATE TABLE gprtm.link_customer_log_20231022 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-22 00:00:00') TO ('2023-10-23 00:00:00');


-- gprtm.link_customer_log_20231023 definition

CREATE TABLE gprtm.link_customer_log_20231023 PARTITION OF gprtm.link_customer_log  FOR VALUES FROM ('2023-10-23 00:00:00') TO ('2023-10-24 00:00:00');


-- gprtm.marketing_customer_log_20221017 definition

CREATE TABLE gprtm.marketing_customer_log_20221017 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-10-17 00:00:00') TO ('2022-10-18 00:00:00');


-- gprtm.marketing_customer_log_20221018 definition

CREATE TABLE gprtm.marketing_customer_log_20221018 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-10-18 00:00:00') TO ('2022-10-19 00:00:00');


-- gprtm.marketing_customer_log_20221019 definition

CREATE TABLE gprtm.marketing_customer_log_20221019 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-10-19 00:00:00') TO ('2022-10-20 00:00:00');


-- gprtm.marketing_customer_log_20221020 definition

CREATE TABLE gprtm.marketing_customer_log_20221020 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-10-20 00:00:00') TO ('2022-10-21 00:00:00');


-- gprtm.marketing_customer_log_20221021 definition

CREATE TABLE gprtm.marketing_customer_log_20221021 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-10-21 00:00:00') TO ('2022-10-22 00:00:00');


-- gprtm.marketing_customer_log_20221022 definition

CREATE TABLE gprtm.marketing_customer_log_20221022 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-10-22 00:00:00') TO ('2022-10-23 00:00:00');


-- gprtm.marketing_customer_log_20221023 definition

CREATE TABLE gprtm.marketing_customer_log_20221023 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-10-23 00:00:00') TO ('2022-10-24 00:00:00');


-- gprtm.marketing_customer_log_20221024 definition

CREATE TABLE gprtm.marketing_customer_log_20221024 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-10-24 00:00:00') TO ('2022-10-25 00:00:00');


-- gprtm.marketing_customer_log_20221025 definition

CREATE TABLE gprtm.marketing_customer_log_20221025 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-10-25 00:00:00') TO ('2022-10-26 00:00:00');


-- gprtm.marketing_customer_log_20221026 definition

CREATE TABLE gprtm.marketing_customer_log_20221026 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-10-26 00:00:00') TO ('2022-10-27 00:00:00');


-- gprtm.marketing_customer_log_20221027 definition

CREATE TABLE gprtm.marketing_customer_log_20221027 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-10-27 00:00:00') TO ('2022-10-28 00:00:00');


-- gprtm.marketing_customer_log_20221028 definition

CREATE TABLE gprtm.marketing_customer_log_20221028 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-10-28 00:00:00') TO ('2022-10-29 00:00:00');


-- gprtm.marketing_customer_log_20221029 definition

CREATE TABLE gprtm.marketing_customer_log_20221029 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-10-29 00:00:00') TO ('2022-10-30 00:00:00');


-- gprtm.marketing_customer_log_20221030 definition

CREATE TABLE gprtm.marketing_customer_log_20221030 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-10-30 00:00:00') TO ('2022-10-31 00:00:00');


-- gprtm.marketing_customer_log_20221031 definition

CREATE TABLE gprtm.marketing_customer_log_20221031 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-10-31 00:00:00') TO ('2022-11-01 00:00:00');


-- gprtm.marketing_customer_log_20221101 definition

CREATE TABLE gprtm.marketing_customer_log_20221101 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-01 00:00:00') TO ('2022-11-02 00:00:00');


-- gprtm.marketing_customer_log_20221102 definition

CREATE TABLE gprtm.marketing_customer_log_20221102 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-02 00:00:00') TO ('2022-11-03 00:00:00');


-- gprtm.marketing_customer_log_20221103 definition

CREATE TABLE gprtm.marketing_customer_log_20221103 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-03 00:00:00') TO ('2022-11-04 00:00:00');


-- gprtm.marketing_customer_log_20221104 definition

CREATE TABLE gprtm.marketing_customer_log_20221104 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-04 00:00:00') TO ('2022-11-05 00:00:00');


-- gprtm.marketing_customer_log_20221105 definition

CREATE TABLE gprtm.marketing_customer_log_20221105 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-05 00:00:00') TO ('2022-11-06 00:00:00');


-- gprtm.marketing_customer_log_20221106 definition

CREATE TABLE gprtm.marketing_customer_log_20221106 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-06 00:00:00') TO ('2022-11-07 00:00:00');


-- gprtm.marketing_customer_log_20221107 definition

CREATE TABLE gprtm.marketing_customer_log_20221107 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-07 00:00:00') TO ('2022-11-08 00:00:00');


-- gprtm.marketing_customer_log_20221108 definition

CREATE TABLE gprtm.marketing_customer_log_20221108 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-08 00:00:00') TO ('2022-11-09 00:00:00');


-- gprtm.marketing_customer_log_20221109 definition

CREATE TABLE gprtm.marketing_customer_log_20221109 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-09 00:00:00') TO ('2022-11-10 00:00:00');


-- gprtm.marketing_customer_log_20221110 definition

CREATE TABLE gprtm.marketing_customer_log_20221110 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-10 00:00:00') TO ('2022-11-11 00:00:00');


-- gprtm.marketing_customer_log_20221111 definition

CREATE TABLE gprtm.marketing_customer_log_20221111 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-11 00:00:00') TO ('2022-11-12 00:00:00');


-- gprtm.marketing_customer_log_20221112 definition

CREATE TABLE gprtm.marketing_customer_log_20221112 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-12 00:00:00') TO ('2022-11-13 00:00:00');


-- gprtm.marketing_customer_log_20221113 definition

CREATE TABLE gprtm.marketing_customer_log_20221113 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-13 00:00:00') TO ('2022-11-14 00:00:00');


-- gprtm.marketing_customer_log_20221114 definition

CREATE TABLE gprtm.marketing_customer_log_20221114 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-14 00:00:00') TO ('2022-11-15 00:00:00');


-- gprtm.marketing_customer_log_20221115 definition

CREATE TABLE gprtm.marketing_customer_log_20221115 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-15 00:00:00') TO ('2022-11-16 00:00:00');


-- gprtm.marketing_customer_log_20221116 definition

CREATE TABLE gprtm.marketing_customer_log_20221116 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-16 00:00:00') TO ('2022-11-17 00:00:00');


-- gprtm.marketing_customer_log_20221117 definition

CREATE TABLE gprtm.marketing_customer_log_20221117 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-17 00:00:00') TO ('2022-11-18 00:00:00');


-- gprtm.marketing_customer_log_20221118 definition

CREATE TABLE gprtm.marketing_customer_log_20221118 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-18 00:00:00') TO ('2022-11-19 00:00:00');


-- gprtm.marketing_customer_log_20221119 definition

CREATE TABLE gprtm.marketing_customer_log_20221119 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-19 00:00:00') TO ('2022-11-20 00:00:00');


-- gprtm.marketing_customer_log_20221120 definition

CREATE TABLE gprtm.marketing_customer_log_20221120 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-20 00:00:00') TO ('2022-11-21 00:00:00');


-- gprtm.marketing_customer_log_20221121 definition

CREATE TABLE gprtm.marketing_customer_log_20221121 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-21 00:00:00') TO ('2022-11-22 00:00:00');


-- gprtm.marketing_customer_log_20221122 definition

CREATE TABLE gprtm.marketing_customer_log_20221122 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-22 00:00:00') TO ('2022-11-23 00:00:00');


-- gprtm.marketing_customer_log_20221123 definition

CREATE TABLE gprtm.marketing_customer_log_20221123 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-23 00:00:00') TO ('2022-11-24 00:00:00');


-- gprtm.marketing_customer_log_20221124 definition

CREATE TABLE gprtm.marketing_customer_log_20221124 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-24 00:00:00') TO ('2022-11-25 00:00:00');


-- gprtm.marketing_customer_log_20221125 definition

CREATE TABLE gprtm.marketing_customer_log_20221125 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-25 00:00:00') TO ('2022-11-26 00:00:00');


-- gprtm.marketing_customer_log_20221126 definition

CREATE TABLE gprtm.marketing_customer_log_20221126 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-26 00:00:00') TO ('2022-11-27 00:00:00');


-- gprtm.marketing_customer_log_20221127 definition

CREATE TABLE gprtm.marketing_customer_log_20221127 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-27 00:00:00') TO ('2022-11-28 00:00:00');


-- gprtm.marketing_customer_log_20221128 definition

CREATE TABLE gprtm.marketing_customer_log_20221128 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-28 00:00:00') TO ('2022-11-29 00:00:00');


-- gprtm.marketing_customer_log_20221129 definition

CREATE TABLE gprtm.marketing_customer_log_20221129 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-29 00:00:00') TO ('2022-11-30 00:00:00');


-- gprtm.marketing_customer_log_20221130 definition

CREATE TABLE gprtm.marketing_customer_log_20221130 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-11-30 00:00:00') TO ('2022-12-01 00:00:00');


-- gprtm.marketing_customer_log_20221201 definition

CREATE TABLE gprtm.marketing_customer_log_20221201 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-01 00:00:00') TO ('2022-12-02 00:00:00');


-- gprtm.marketing_customer_log_20221202 definition

CREATE TABLE gprtm.marketing_customer_log_20221202 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-02 00:00:00') TO ('2022-12-03 00:00:00');


-- gprtm.marketing_customer_log_20221203 definition

CREATE TABLE gprtm.marketing_customer_log_20221203 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-03 00:00:00') TO ('2022-12-04 00:00:00');


-- gprtm.marketing_customer_log_20221204 definition

CREATE TABLE gprtm.marketing_customer_log_20221204 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-04 00:00:00') TO ('2022-12-05 00:00:00');


-- gprtm.marketing_customer_log_20221205 definition

CREATE TABLE gprtm.marketing_customer_log_20221205 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-05 00:00:00') TO ('2022-12-06 00:00:00');


-- gprtm.marketing_customer_log_20221206 definition

CREATE TABLE gprtm.marketing_customer_log_20221206 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-06 00:00:00') TO ('2022-12-07 00:00:00');


-- gprtm.marketing_customer_log_20221207 definition

CREATE TABLE gprtm.marketing_customer_log_20221207 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-07 00:00:00') TO ('2022-12-08 00:00:00');


-- gprtm.marketing_customer_log_20221208 definition

CREATE TABLE gprtm.marketing_customer_log_20221208 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-08 00:00:00') TO ('2022-12-09 00:00:00');


-- gprtm.marketing_customer_log_20221209 definition

CREATE TABLE gprtm.marketing_customer_log_20221209 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-09 00:00:00') TO ('2022-12-10 00:00:00');


-- gprtm.marketing_customer_log_20221210 definition

CREATE TABLE gprtm.marketing_customer_log_20221210 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-10 00:00:00') TO ('2022-12-11 00:00:00');


-- gprtm.marketing_customer_log_20221211 definition

CREATE TABLE gprtm.marketing_customer_log_20221211 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-11 00:00:00') TO ('2022-12-12 00:00:00');


-- gprtm.marketing_customer_log_20221212 definition

CREATE TABLE gprtm.marketing_customer_log_20221212 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-12 00:00:00') TO ('2022-12-13 00:00:00');


-- gprtm.marketing_customer_log_20221213 definition

CREATE TABLE gprtm.marketing_customer_log_20221213 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-13 00:00:00') TO ('2022-12-14 00:00:00');


-- gprtm.marketing_customer_log_20221214 definition

CREATE TABLE gprtm.marketing_customer_log_20221214 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-14 00:00:00') TO ('2022-12-15 00:00:00');


-- gprtm.marketing_customer_log_20221215 definition

CREATE TABLE gprtm.marketing_customer_log_20221215 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-15 00:00:00') TO ('2022-12-16 00:00:00');


-- gprtm.marketing_customer_log_20221216 definition

CREATE TABLE gprtm.marketing_customer_log_20221216 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-16 00:00:00') TO ('2022-12-17 00:00:00');


-- gprtm.marketing_customer_log_20221217 definition

CREATE TABLE gprtm.marketing_customer_log_20221217 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-17 00:00:00') TO ('2022-12-18 00:00:00');


-- gprtm.marketing_customer_log_20221218 definition

CREATE TABLE gprtm.marketing_customer_log_20221218 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-18 00:00:00') TO ('2022-12-19 00:00:00');


-- gprtm.marketing_customer_log_20221219 definition

CREATE TABLE gprtm.marketing_customer_log_20221219 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-19 00:00:00') TO ('2022-12-20 00:00:00');


-- gprtm.marketing_customer_log_20221220 definition

CREATE TABLE gprtm.marketing_customer_log_20221220 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-20 00:00:00') TO ('2022-12-21 00:00:00');


-- gprtm.marketing_customer_log_20221221 definition

CREATE TABLE gprtm.marketing_customer_log_20221221 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-21 00:00:00') TO ('2022-12-22 00:00:00');


-- gprtm.marketing_customer_log_20221222 definition

CREATE TABLE gprtm.marketing_customer_log_20221222 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-22 00:00:00') TO ('2022-12-23 00:00:00');


-- gprtm.marketing_customer_log_20221223 definition

CREATE TABLE gprtm.marketing_customer_log_20221223 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-23 00:00:00') TO ('2022-12-24 00:00:00');


-- gprtm.marketing_customer_log_20221224 definition

CREATE TABLE gprtm.marketing_customer_log_20221224 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-24 00:00:00') TO ('2022-12-25 00:00:00');


-- gprtm.marketing_customer_log_20221225 definition

CREATE TABLE gprtm.marketing_customer_log_20221225 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-25 00:00:00') TO ('2022-12-26 00:00:00');


-- gprtm.marketing_customer_log_20221226 definition

CREATE TABLE gprtm.marketing_customer_log_20221226 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-26 00:00:00') TO ('2022-12-27 00:00:00');


-- gprtm.marketing_customer_log_20221227 definition

CREATE TABLE gprtm.marketing_customer_log_20221227 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-27 00:00:00') TO ('2022-12-28 00:00:00');


-- gprtm.marketing_customer_log_20221228 definition

CREATE TABLE gprtm.marketing_customer_log_20221228 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-28 00:00:00') TO ('2022-12-29 00:00:00');


-- gprtm.marketing_customer_log_20221229 definition

CREATE TABLE gprtm.marketing_customer_log_20221229 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-29 00:00:00') TO ('2022-12-30 00:00:00');


-- gprtm.marketing_customer_log_20221230 definition

CREATE TABLE gprtm.marketing_customer_log_20221230 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-30 00:00:00') TO ('2022-12-31 00:00:00');


-- gprtm.marketing_customer_log_20221231 definition

CREATE TABLE gprtm.marketing_customer_log_20221231 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2022-12-31 00:00:00') TO ('2023-01-01 00:00:00');


-- gprtm.marketing_customer_log_20230101 definition

CREATE TABLE gprtm.marketing_customer_log_20230101 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-01 00:00:00') TO ('2023-01-02 00:00:00');


-- gprtm.marketing_customer_log_20230102 definition

CREATE TABLE gprtm.marketing_customer_log_20230102 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-02 00:00:00') TO ('2023-01-03 00:00:00');


-- gprtm.marketing_customer_log_20230103 definition

CREATE TABLE gprtm.marketing_customer_log_20230103 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-03 00:00:00') TO ('2023-01-04 00:00:00');


-- gprtm.marketing_customer_log_20230104 definition

CREATE TABLE gprtm.marketing_customer_log_20230104 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-04 00:00:00') TO ('2023-01-05 00:00:00');


-- gprtm.marketing_customer_log_20230105 definition

CREATE TABLE gprtm.marketing_customer_log_20230105 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-05 00:00:00') TO ('2023-01-06 00:00:00');


-- gprtm.marketing_customer_log_20230106 definition

CREATE TABLE gprtm.marketing_customer_log_20230106 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-06 00:00:00') TO ('2023-01-07 00:00:00');


-- gprtm.marketing_customer_log_20230107 definition

CREATE TABLE gprtm.marketing_customer_log_20230107 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-07 00:00:00') TO ('2023-01-08 00:00:00');


-- gprtm.marketing_customer_log_20230108 definition

CREATE TABLE gprtm.marketing_customer_log_20230108 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-08 00:00:00') TO ('2023-01-09 00:00:00');


-- gprtm.marketing_customer_log_20230109 definition

CREATE TABLE gprtm.marketing_customer_log_20230109 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-09 00:00:00') TO ('2023-01-10 00:00:00');


-- gprtm.marketing_customer_log_20230110 definition

CREATE TABLE gprtm.marketing_customer_log_20230110 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-10 00:00:00') TO ('2023-01-11 00:00:00');


-- gprtm.marketing_customer_log_20230111 definition

CREATE TABLE gprtm.marketing_customer_log_20230111 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-11 00:00:00') TO ('2023-01-12 00:00:00');


-- gprtm.marketing_customer_log_20230112 definition

CREATE TABLE gprtm.marketing_customer_log_20230112 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-12 00:00:00') TO ('2023-01-13 00:00:00');


-- gprtm.marketing_customer_log_20230113 definition

CREATE TABLE gprtm.marketing_customer_log_20230113 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-13 00:00:00') TO ('2023-01-14 00:00:00');


-- gprtm.marketing_customer_log_20230114 definition

CREATE TABLE gprtm.marketing_customer_log_20230114 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-14 00:00:00') TO ('2023-01-15 00:00:00');


-- gprtm.marketing_customer_log_20230115 definition

CREATE TABLE gprtm.marketing_customer_log_20230115 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-15 00:00:00') TO ('2023-01-16 00:00:00');


-- gprtm.marketing_customer_log_20230116 definition

CREATE TABLE gprtm.marketing_customer_log_20230116 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-16 00:00:00') TO ('2023-01-17 00:00:00');


-- gprtm.marketing_customer_log_20230117 definition

CREATE TABLE gprtm.marketing_customer_log_20230117 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-17 00:00:00') TO ('2023-01-18 00:00:00');


-- gprtm.marketing_customer_log_20230118 definition

CREATE TABLE gprtm.marketing_customer_log_20230118 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-18 00:00:00') TO ('2023-01-19 00:00:00');


-- gprtm.marketing_customer_log_20230119 definition

CREATE TABLE gprtm.marketing_customer_log_20230119 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-19 00:00:00') TO ('2023-01-20 00:00:00');


-- gprtm.marketing_customer_log_20230120 definition

CREATE TABLE gprtm.marketing_customer_log_20230120 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-20 00:00:00') TO ('2023-01-21 00:00:00');


-- gprtm.marketing_customer_log_20230121 definition

CREATE TABLE gprtm.marketing_customer_log_20230121 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-21 00:00:00') TO ('2023-01-22 00:00:00');


-- gprtm.marketing_customer_log_20230122 definition

CREATE TABLE gprtm.marketing_customer_log_20230122 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-22 00:00:00') TO ('2023-01-23 00:00:00');


-- gprtm.marketing_customer_log_20230123 definition

CREATE TABLE gprtm.marketing_customer_log_20230123 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-23 00:00:00') TO ('2023-01-24 00:00:00');


-- gprtm.marketing_customer_log_20230124 definition

CREATE TABLE gprtm.marketing_customer_log_20230124 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-24 00:00:00') TO ('2023-01-25 00:00:00');


-- gprtm.marketing_customer_log_20230125 definition

CREATE TABLE gprtm.marketing_customer_log_20230125 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-25 00:00:00') TO ('2023-01-26 00:00:00');


-- gprtm.marketing_customer_log_20230126 definition

CREATE TABLE gprtm.marketing_customer_log_20230126 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-26 00:00:00') TO ('2023-01-27 00:00:00');


-- gprtm.marketing_customer_log_20230127 definition

CREATE TABLE gprtm.marketing_customer_log_20230127 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-27 00:00:00') TO ('2023-01-28 00:00:00');


-- gprtm.marketing_customer_log_20230128 definition

CREATE TABLE gprtm.marketing_customer_log_20230128 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-28 00:00:00') TO ('2023-01-29 00:00:00');


-- gprtm.marketing_customer_log_20230129 definition

CREATE TABLE gprtm.marketing_customer_log_20230129 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-29 00:00:00') TO ('2023-01-30 00:00:00');


-- gprtm.marketing_customer_log_20230130 definition

CREATE TABLE gprtm.marketing_customer_log_20230130 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-30 00:00:00') TO ('2023-01-31 00:00:00');


-- gprtm.marketing_customer_log_20230131 definition

CREATE TABLE gprtm.marketing_customer_log_20230131 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-01-31 00:00:00') TO ('2023-02-01 00:00:00');


-- gprtm.marketing_customer_log_20230201 definition

CREATE TABLE gprtm.marketing_customer_log_20230201 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-01 00:00:00') TO ('2023-02-02 00:00:00');


-- gprtm.marketing_customer_log_20230202 definition

CREATE TABLE gprtm.marketing_customer_log_20230202 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-02 00:00:00') TO ('2023-02-03 00:00:00');


-- gprtm.marketing_customer_log_20230203 definition

CREATE TABLE gprtm.marketing_customer_log_20230203 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-03 00:00:00') TO ('2023-02-04 00:00:00');


-- gprtm.marketing_customer_log_20230204 definition

CREATE TABLE gprtm.marketing_customer_log_20230204 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-04 00:00:00') TO ('2023-02-05 00:00:00');


-- gprtm.marketing_customer_log_20230205 definition

CREATE TABLE gprtm.marketing_customer_log_20230205 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-05 00:00:00') TO ('2023-02-06 00:00:00');


-- gprtm.marketing_customer_log_20230206 definition

CREATE TABLE gprtm.marketing_customer_log_20230206 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-06 00:00:00') TO ('2023-02-07 00:00:00');


-- gprtm.marketing_customer_log_20230207 definition

CREATE TABLE gprtm.marketing_customer_log_20230207 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-07 00:00:00') TO ('2023-02-08 00:00:00');


-- gprtm.marketing_customer_log_20230208 definition

CREATE TABLE gprtm.marketing_customer_log_20230208 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-08 00:00:00') TO ('2023-02-09 00:00:00');


-- gprtm.marketing_customer_log_20230209 definition

CREATE TABLE gprtm.marketing_customer_log_20230209 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-09 00:00:00') TO ('2023-02-10 00:00:00');


-- gprtm.marketing_customer_log_20230210 definition

CREATE TABLE gprtm.marketing_customer_log_20230210 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-10 00:00:00') TO ('2023-02-11 00:00:00');


-- gprtm.marketing_customer_log_20230211 definition

CREATE TABLE gprtm.marketing_customer_log_20230211 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-11 00:00:00') TO ('2023-02-12 00:00:00');


-- gprtm.marketing_customer_log_20230212 definition

CREATE TABLE gprtm.marketing_customer_log_20230212 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-12 00:00:00') TO ('2023-02-13 00:00:00');


-- gprtm.marketing_customer_log_20230213 definition

CREATE TABLE gprtm.marketing_customer_log_20230213 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-13 00:00:00') TO ('2023-02-14 00:00:00');


-- gprtm.marketing_customer_log_20230214 definition

CREATE TABLE gprtm.marketing_customer_log_20230214 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-14 00:00:00') TO ('2023-02-15 00:00:00');


-- gprtm.marketing_customer_log_20230215 definition

CREATE TABLE gprtm.marketing_customer_log_20230215 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-15 00:00:00') TO ('2023-02-16 00:00:00');


-- gprtm.marketing_customer_log_20230216 definition

CREATE TABLE gprtm.marketing_customer_log_20230216 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-16 00:00:00') TO ('2023-02-17 00:00:00');


-- gprtm.marketing_customer_log_20230217 definition

CREATE TABLE gprtm.marketing_customer_log_20230217 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-17 00:00:00') TO ('2023-02-18 00:00:00');


-- gprtm.marketing_customer_log_20230218 definition

CREATE TABLE gprtm.marketing_customer_log_20230218 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-18 00:00:00') TO ('2023-02-19 00:00:00');


-- gprtm.marketing_customer_log_20230219 definition

CREATE TABLE gprtm.marketing_customer_log_20230219 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-19 00:00:00') TO ('2023-02-20 00:00:00');


-- gprtm.marketing_customer_log_20230220 definition

CREATE TABLE gprtm.marketing_customer_log_20230220 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-20 00:00:00') TO ('2023-02-21 00:00:00');


-- gprtm.marketing_customer_log_20230221 definition

CREATE TABLE gprtm.marketing_customer_log_20230221 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-21 00:00:00') TO ('2023-02-22 00:00:00');


-- gprtm.marketing_customer_log_20230222 definition

CREATE TABLE gprtm.marketing_customer_log_20230222 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-22 00:00:00') TO ('2023-02-23 00:00:00');


-- gprtm.marketing_customer_log_20230223 definition

CREATE TABLE gprtm.marketing_customer_log_20230223 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-23 00:00:00') TO ('2023-02-24 00:00:00');


-- gprtm.marketing_customer_log_20230224 definition

CREATE TABLE gprtm.marketing_customer_log_20230224 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-24 00:00:00') TO ('2023-02-25 00:00:00');


-- gprtm.marketing_customer_log_20230225 definition

CREATE TABLE gprtm.marketing_customer_log_20230225 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-25 00:00:00') TO ('2023-02-26 00:00:00');


-- gprtm.marketing_customer_log_20230226 definition

CREATE TABLE gprtm.marketing_customer_log_20230226 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-26 00:00:00') TO ('2023-02-27 00:00:00');


-- gprtm.marketing_customer_log_20230227 definition

CREATE TABLE gprtm.marketing_customer_log_20230227 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-27 00:00:00') TO ('2023-02-28 00:00:00');


-- gprtm.marketing_customer_log_20230228 definition

CREATE TABLE gprtm.marketing_customer_log_20230228 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-02-28 00:00:00') TO ('2023-03-01 00:00:00');


-- gprtm.marketing_customer_log_20230301 definition

CREATE TABLE gprtm.marketing_customer_log_20230301 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-01 00:00:00') TO ('2023-03-02 00:00:00');


-- gprtm.marketing_customer_log_20230302 definition

CREATE TABLE gprtm.marketing_customer_log_20230302 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-02 00:00:00') TO ('2023-03-03 00:00:00');


-- gprtm.marketing_customer_log_20230303 definition

CREATE TABLE gprtm.marketing_customer_log_20230303 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-03 00:00:00') TO ('2023-03-04 00:00:00');


-- gprtm.marketing_customer_log_20230304 definition

CREATE TABLE gprtm.marketing_customer_log_20230304 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-04 00:00:00') TO ('2023-03-05 00:00:00');


-- gprtm.marketing_customer_log_20230305 definition

CREATE TABLE gprtm.marketing_customer_log_20230305 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-05 00:00:00') TO ('2023-03-06 00:00:00');


-- gprtm.marketing_customer_log_20230306 definition

CREATE TABLE gprtm.marketing_customer_log_20230306 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-06 00:00:00') TO ('2023-03-07 00:00:00');


-- gprtm.marketing_customer_log_20230307 definition

CREATE TABLE gprtm.marketing_customer_log_20230307 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-07 00:00:00') TO ('2023-03-08 00:00:00');


-- gprtm.marketing_customer_log_20230308 definition

CREATE TABLE gprtm.marketing_customer_log_20230308 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-08 00:00:00') TO ('2023-03-09 00:00:00');


-- gprtm.marketing_customer_log_20230309 definition

CREATE TABLE gprtm.marketing_customer_log_20230309 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-09 00:00:00') TO ('2023-03-10 00:00:00');


-- gprtm.marketing_customer_log_20230310 definition

CREATE TABLE gprtm.marketing_customer_log_20230310 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-10 00:00:00') TO ('2023-03-11 00:00:00');


-- gprtm.marketing_customer_log_20230311 definition

CREATE TABLE gprtm.marketing_customer_log_20230311 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-11 00:00:00') TO ('2023-03-12 00:00:00');


-- gprtm.marketing_customer_log_20230312 definition

CREATE TABLE gprtm.marketing_customer_log_20230312 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-12 00:00:00') TO ('2023-03-13 00:00:00');


-- gprtm.marketing_customer_log_20230313 definition

CREATE TABLE gprtm.marketing_customer_log_20230313 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-13 00:00:00') TO ('2023-03-14 00:00:00');


-- gprtm.marketing_customer_log_20230314 definition

CREATE TABLE gprtm.marketing_customer_log_20230314 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-14 00:00:00') TO ('2023-03-15 00:00:00');


-- gprtm.marketing_customer_log_20230315 definition

CREATE TABLE gprtm.marketing_customer_log_20230315 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-15 00:00:00') TO ('2023-03-16 00:00:00');


-- gprtm.marketing_customer_log_20230316 definition

CREATE TABLE gprtm.marketing_customer_log_20230316 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-16 00:00:00') TO ('2023-03-17 00:00:00');


-- gprtm.marketing_customer_log_20230317 definition

CREATE TABLE gprtm.marketing_customer_log_20230317 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-17 00:00:00') TO ('2023-03-18 00:00:00');


-- gprtm.marketing_customer_log_20230318 definition

CREATE TABLE gprtm.marketing_customer_log_20230318 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-18 00:00:00') TO ('2023-03-19 00:00:00');


-- gprtm.marketing_customer_log_20230319 definition

CREATE TABLE gprtm.marketing_customer_log_20230319 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-19 00:00:00') TO ('2023-03-20 00:00:00');


-- gprtm.marketing_customer_log_20230320 definition

CREATE TABLE gprtm.marketing_customer_log_20230320 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-20 00:00:00') TO ('2023-03-21 00:00:00');


-- gprtm.marketing_customer_log_20230321 definition

CREATE TABLE gprtm.marketing_customer_log_20230321 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-21 00:00:00') TO ('2023-03-22 00:00:00');


-- gprtm.marketing_customer_log_20230322 definition

CREATE TABLE gprtm.marketing_customer_log_20230322 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-22 00:00:00') TO ('2023-03-23 00:00:00');


-- gprtm.marketing_customer_log_20230323 definition

CREATE TABLE gprtm.marketing_customer_log_20230323 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-23 00:00:00') TO ('2023-03-24 00:00:00');


-- gprtm.marketing_customer_log_20230324 definition

CREATE TABLE gprtm.marketing_customer_log_20230324 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-24 00:00:00') TO ('2023-03-25 00:00:00');


-- gprtm.marketing_customer_log_20230325 definition

CREATE TABLE gprtm.marketing_customer_log_20230325 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-25 00:00:00') TO ('2023-03-26 00:00:00');


-- gprtm.marketing_customer_log_20230326 definition

CREATE TABLE gprtm.marketing_customer_log_20230326 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-26 00:00:00') TO ('2023-03-27 00:00:00');


-- gprtm.marketing_customer_log_20230327 definition

CREATE TABLE gprtm.marketing_customer_log_20230327 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-27 00:00:00') TO ('2023-03-28 00:00:00');


-- gprtm.marketing_customer_log_20230328 definition

CREATE TABLE gprtm.marketing_customer_log_20230328 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-28 00:00:00') TO ('2023-03-29 00:00:00');


-- gprtm.marketing_customer_log_20230329 definition

CREATE TABLE gprtm.marketing_customer_log_20230329 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-29 00:00:00') TO ('2023-03-30 00:00:00');


-- gprtm.marketing_customer_log_20230330 definition

CREATE TABLE gprtm.marketing_customer_log_20230330 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-30 00:00:00') TO ('2023-03-31 00:00:00');


-- gprtm.marketing_customer_log_20230331 definition

CREATE TABLE gprtm.marketing_customer_log_20230331 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-03-31 00:00:00') TO ('2023-04-01 00:00:00');


-- gprtm.marketing_customer_log_20230401 definition

CREATE TABLE gprtm.marketing_customer_log_20230401 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-01 00:00:00') TO ('2023-04-02 00:00:00');


-- gprtm.marketing_customer_log_20230402 definition

CREATE TABLE gprtm.marketing_customer_log_20230402 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-02 00:00:00') TO ('2023-04-03 00:00:00');


-- gprtm.marketing_customer_log_20230403 definition

CREATE TABLE gprtm.marketing_customer_log_20230403 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-03 00:00:00') TO ('2023-04-04 00:00:00');


-- gprtm.marketing_customer_log_20230404 definition

CREATE TABLE gprtm.marketing_customer_log_20230404 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-04 00:00:00') TO ('2023-04-05 00:00:00');


-- gprtm.marketing_customer_log_20230405 definition

CREATE TABLE gprtm.marketing_customer_log_20230405 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-05 00:00:00') TO ('2023-04-06 00:00:00');


-- gprtm.marketing_customer_log_20230406 definition

CREATE TABLE gprtm.marketing_customer_log_20230406 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-06 00:00:00') TO ('2023-04-07 00:00:00');


-- gprtm.marketing_customer_log_20230407 definition

CREATE TABLE gprtm.marketing_customer_log_20230407 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-07 00:00:00') TO ('2023-04-08 00:00:00');


-- gprtm.marketing_customer_log_20230408 definition

CREATE TABLE gprtm.marketing_customer_log_20230408 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-08 00:00:00') TO ('2023-04-09 00:00:00');


-- gprtm.marketing_customer_log_20230409 definition

CREATE TABLE gprtm.marketing_customer_log_20230409 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-09 00:00:00') TO ('2023-04-10 00:00:00');


-- gprtm.marketing_customer_log_20230410 definition

CREATE TABLE gprtm.marketing_customer_log_20230410 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-10 00:00:00') TO ('2023-04-11 00:00:00');


-- gprtm.marketing_customer_log_20230411 definition

CREATE TABLE gprtm.marketing_customer_log_20230411 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-11 00:00:00') TO ('2023-04-12 00:00:00');


-- gprtm.marketing_customer_log_20230412 definition

CREATE TABLE gprtm.marketing_customer_log_20230412 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-12 00:00:00') TO ('2023-04-13 00:00:00');


-- gprtm.marketing_customer_log_20230413 definition

CREATE TABLE gprtm.marketing_customer_log_20230413 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-13 00:00:00') TO ('2023-04-14 00:00:00');


-- gprtm.marketing_customer_log_20230414 definition

CREATE TABLE gprtm.marketing_customer_log_20230414 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-14 00:00:00') TO ('2023-04-15 00:00:00');


-- gprtm.marketing_customer_log_20230415 definition

CREATE TABLE gprtm.marketing_customer_log_20230415 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-15 00:00:00') TO ('2023-04-16 00:00:00');


-- gprtm.marketing_customer_log_20230416 definition

CREATE TABLE gprtm.marketing_customer_log_20230416 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-16 00:00:00') TO ('2023-04-17 00:00:00');


-- gprtm.marketing_customer_log_20230417 definition

CREATE TABLE gprtm.marketing_customer_log_20230417 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-17 00:00:00') TO ('2023-04-18 00:00:00');


-- gprtm.marketing_customer_log_20230418 definition

CREATE TABLE gprtm.marketing_customer_log_20230418 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-18 00:00:00') TO ('2023-04-19 00:00:00');


-- gprtm.marketing_customer_log_20230419 definition

CREATE TABLE gprtm.marketing_customer_log_20230419 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-19 00:00:00') TO ('2023-04-20 00:00:00');


-- gprtm.marketing_customer_log_20230420 definition

CREATE TABLE gprtm.marketing_customer_log_20230420 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-20 00:00:00') TO ('2023-04-21 00:00:00');


-- gprtm.marketing_customer_log_20230421 definition

CREATE TABLE gprtm.marketing_customer_log_20230421 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-21 00:00:00') TO ('2023-04-22 00:00:00');


-- gprtm.marketing_customer_log_20230422 definition

CREATE TABLE gprtm.marketing_customer_log_20230422 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-22 00:00:00') TO ('2023-04-23 00:00:00');


-- gprtm.marketing_customer_log_20230423 definition

CREATE TABLE gprtm.marketing_customer_log_20230423 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-23 00:00:00') TO ('2023-04-24 00:00:00');


-- gprtm.marketing_customer_log_20230424 definition

CREATE TABLE gprtm.marketing_customer_log_20230424 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-24 00:00:00') TO ('2023-04-25 00:00:00');


-- gprtm.marketing_customer_log_20230425 definition

CREATE TABLE gprtm.marketing_customer_log_20230425 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-25 00:00:00') TO ('2023-04-26 00:00:00');


-- gprtm.marketing_customer_log_20230426 definition

CREATE TABLE gprtm.marketing_customer_log_20230426 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-26 00:00:00') TO ('2023-04-27 00:00:00');


-- gprtm.marketing_customer_log_20230427 definition

CREATE TABLE gprtm.marketing_customer_log_20230427 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-27 00:00:00') TO ('2023-04-28 00:00:00');


-- gprtm.marketing_customer_log_20230428 definition

CREATE TABLE gprtm.marketing_customer_log_20230428 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-28 00:00:00') TO ('2023-04-29 00:00:00');


-- gprtm.marketing_customer_log_20230429 definition

CREATE TABLE gprtm.marketing_customer_log_20230429 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-29 00:00:00') TO ('2023-04-30 00:00:00');


-- gprtm.marketing_customer_log_20230430 definition

CREATE TABLE gprtm.marketing_customer_log_20230430 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-04-30 00:00:00') TO ('2023-05-01 00:00:00');


-- gprtm.marketing_customer_log_20230501 definition

CREATE TABLE gprtm.marketing_customer_log_20230501 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-01 00:00:00') TO ('2023-05-02 00:00:00');


-- gprtm.marketing_customer_log_20230502 definition

CREATE TABLE gprtm.marketing_customer_log_20230502 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-02 00:00:00') TO ('2023-05-03 00:00:00');


-- gprtm.marketing_customer_log_20230503 definition

CREATE TABLE gprtm.marketing_customer_log_20230503 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-03 00:00:00') TO ('2023-05-04 00:00:00');


-- gprtm.marketing_customer_log_20230504 definition

CREATE TABLE gprtm.marketing_customer_log_20230504 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-04 00:00:00') TO ('2023-05-05 00:00:00');


-- gprtm.marketing_customer_log_20230505 definition

CREATE TABLE gprtm.marketing_customer_log_20230505 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-05 00:00:00') TO ('2023-05-06 00:00:00');


-- gprtm.marketing_customer_log_20230506 definition

CREATE TABLE gprtm.marketing_customer_log_20230506 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-06 00:00:00') TO ('2023-05-07 00:00:00');


-- gprtm.marketing_customer_log_20230507 definition

CREATE TABLE gprtm.marketing_customer_log_20230507 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-07 00:00:00') TO ('2023-05-08 00:00:00');


-- gprtm.marketing_customer_log_20230508 definition

CREATE TABLE gprtm.marketing_customer_log_20230508 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-08 00:00:00') TO ('2023-05-09 00:00:00');


-- gprtm.marketing_customer_log_20230509 definition

CREATE TABLE gprtm.marketing_customer_log_20230509 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-09 00:00:00') TO ('2023-05-10 00:00:00');


-- gprtm.marketing_customer_log_20230510 definition

CREATE TABLE gprtm.marketing_customer_log_20230510 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-10 00:00:00') TO ('2023-05-11 00:00:00');


-- gprtm.marketing_customer_log_20230511 definition

CREATE TABLE gprtm.marketing_customer_log_20230511 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-11 00:00:00') TO ('2023-05-12 00:00:00');


-- gprtm.marketing_customer_log_20230512 definition

CREATE TABLE gprtm.marketing_customer_log_20230512 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-12 00:00:00') TO ('2023-05-13 00:00:00');


-- gprtm.marketing_customer_log_20230513 definition

CREATE TABLE gprtm.marketing_customer_log_20230513 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-13 00:00:00') TO ('2023-05-14 00:00:00');


-- gprtm.marketing_customer_log_20230514 definition

CREATE TABLE gprtm.marketing_customer_log_20230514 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-14 00:00:00') TO ('2023-05-15 00:00:00');


-- gprtm.marketing_customer_log_20230515 definition

CREATE TABLE gprtm.marketing_customer_log_20230515 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-15 00:00:00') TO ('2023-05-16 00:00:00');


-- gprtm.marketing_customer_log_20230516 definition

CREATE TABLE gprtm.marketing_customer_log_20230516 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-16 00:00:00') TO ('2023-05-17 00:00:00');


-- gprtm.marketing_customer_log_20230517 definition

CREATE TABLE gprtm.marketing_customer_log_20230517 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-17 00:00:00') TO ('2023-05-18 00:00:00');


-- gprtm.marketing_customer_log_20230518 definition

CREATE TABLE gprtm.marketing_customer_log_20230518 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-18 00:00:00') TO ('2023-05-19 00:00:00');


-- gprtm.marketing_customer_log_20230519 definition

CREATE TABLE gprtm.marketing_customer_log_20230519 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-19 00:00:00') TO ('2023-05-20 00:00:00');


-- gprtm.marketing_customer_log_20230520 definition

CREATE TABLE gprtm.marketing_customer_log_20230520 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-20 00:00:00') TO ('2023-05-21 00:00:00');


-- gprtm.marketing_customer_log_20230521 definition

CREATE TABLE gprtm.marketing_customer_log_20230521 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-21 00:00:00') TO ('2023-05-22 00:00:00');


-- gprtm.marketing_customer_log_20230522 definition

CREATE TABLE gprtm.marketing_customer_log_20230522 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-22 00:00:00') TO ('2023-05-23 00:00:00');


-- gprtm.marketing_customer_log_20230523 definition

CREATE TABLE gprtm.marketing_customer_log_20230523 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-23 00:00:00') TO ('2023-05-24 00:00:00');


-- gprtm.marketing_customer_log_20230524 definition

CREATE TABLE gprtm.marketing_customer_log_20230524 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-24 00:00:00') TO ('2023-05-25 00:00:00');


-- gprtm.marketing_customer_log_20230525 definition

CREATE TABLE gprtm.marketing_customer_log_20230525 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-25 00:00:00') TO ('2023-05-26 00:00:00');


-- gprtm.marketing_customer_log_20230526 definition

CREATE TABLE gprtm.marketing_customer_log_20230526 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-26 00:00:00') TO ('2023-05-27 00:00:00');


-- gprtm.marketing_customer_log_20230527 definition

CREATE TABLE gprtm.marketing_customer_log_20230527 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-27 00:00:00') TO ('2023-05-28 00:00:00');


-- gprtm.marketing_customer_log_20230528 definition

CREATE TABLE gprtm.marketing_customer_log_20230528 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-28 00:00:00') TO ('2023-05-29 00:00:00');


-- gprtm.marketing_customer_log_20230529 definition

CREATE TABLE gprtm.marketing_customer_log_20230529 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-29 00:00:00') TO ('2023-05-30 00:00:00');


-- gprtm.marketing_customer_log_20230530 definition

CREATE TABLE gprtm.marketing_customer_log_20230530 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-30 00:00:00') TO ('2023-05-31 00:00:00');


-- gprtm.marketing_customer_log_20230531 definition

CREATE TABLE gprtm.marketing_customer_log_20230531 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-05-31 00:00:00') TO ('2023-06-01 00:00:00');


-- gprtm.marketing_customer_log_20230601 definition

CREATE TABLE gprtm.marketing_customer_log_20230601 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-01 00:00:00') TO ('2023-06-02 00:00:00');


-- gprtm.marketing_customer_log_20230602 definition

CREATE TABLE gprtm.marketing_customer_log_20230602 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-02 00:00:00') TO ('2023-06-03 00:00:00');


-- gprtm.marketing_customer_log_20230603 definition

CREATE TABLE gprtm.marketing_customer_log_20230603 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-03 00:00:00') TO ('2023-06-04 00:00:00');


-- gprtm.marketing_customer_log_20230604 definition

CREATE TABLE gprtm.marketing_customer_log_20230604 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-04 00:00:00') TO ('2023-06-05 00:00:00');


-- gprtm.marketing_customer_log_20230605 definition

CREATE TABLE gprtm.marketing_customer_log_20230605 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-05 00:00:00') TO ('2023-06-06 00:00:00');


-- gprtm.marketing_customer_log_20230606 definition

CREATE TABLE gprtm.marketing_customer_log_20230606 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-06 00:00:00') TO ('2023-06-07 00:00:00');


-- gprtm.marketing_customer_log_20230607 definition

CREATE TABLE gprtm.marketing_customer_log_20230607 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-07 00:00:00') TO ('2023-06-08 00:00:00');


-- gprtm.marketing_customer_log_20230608 definition

CREATE TABLE gprtm.marketing_customer_log_20230608 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-08 00:00:00') TO ('2023-06-09 00:00:00');


-- gprtm.marketing_customer_log_20230609 definition

CREATE TABLE gprtm.marketing_customer_log_20230609 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-09 00:00:00') TO ('2023-06-10 00:00:00');


-- gprtm.marketing_customer_log_20230610 definition

CREATE TABLE gprtm.marketing_customer_log_20230610 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-10 00:00:00') TO ('2023-06-11 00:00:00');


-- gprtm.marketing_customer_log_20230611 definition

CREATE TABLE gprtm.marketing_customer_log_20230611 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-11 00:00:00') TO ('2023-06-12 00:00:00');


-- gprtm.marketing_customer_log_20230612 definition

CREATE TABLE gprtm.marketing_customer_log_20230612 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-12 00:00:00') TO ('2023-06-13 00:00:00');


-- gprtm.marketing_customer_log_20230613 definition

CREATE TABLE gprtm.marketing_customer_log_20230613 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-13 00:00:00') TO ('2023-06-14 00:00:00');


-- gprtm.marketing_customer_log_20230614 definition

CREATE TABLE gprtm.marketing_customer_log_20230614 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-14 00:00:00') TO ('2023-06-15 00:00:00');


-- gprtm.marketing_customer_log_20230615 definition

CREATE TABLE gprtm.marketing_customer_log_20230615 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-15 00:00:00') TO ('2023-06-16 00:00:00');


-- gprtm.marketing_customer_log_20230616 definition

CREATE TABLE gprtm.marketing_customer_log_20230616 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-16 00:00:00') TO ('2023-06-17 00:00:00');


-- gprtm.marketing_customer_log_20230617 definition

CREATE TABLE gprtm.marketing_customer_log_20230617 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-17 00:00:00') TO ('2023-06-18 00:00:00');


-- gprtm.marketing_customer_log_20230618 definition

CREATE TABLE gprtm.marketing_customer_log_20230618 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-18 00:00:00') TO ('2023-06-19 00:00:00');


-- gprtm.marketing_customer_log_20230619 definition

CREATE TABLE gprtm.marketing_customer_log_20230619 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-19 00:00:00') TO ('2023-06-20 00:00:00');


-- gprtm.marketing_customer_log_20230620 definition

CREATE TABLE gprtm.marketing_customer_log_20230620 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-20 00:00:00') TO ('2023-06-21 00:00:00');


-- gprtm.marketing_customer_log_20230621 definition

CREATE TABLE gprtm.marketing_customer_log_20230621 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-21 00:00:00') TO ('2023-06-22 00:00:00');


-- gprtm.marketing_customer_log_20230622 definition

CREATE TABLE gprtm.marketing_customer_log_20230622 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-22 00:00:00') TO ('2023-06-23 00:00:00');


-- gprtm.marketing_customer_log_20230623 definition

CREATE TABLE gprtm.marketing_customer_log_20230623 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-23 00:00:00') TO ('2023-06-24 00:00:00');


-- gprtm.marketing_customer_log_20230624 definition

CREATE TABLE gprtm.marketing_customer_log_20230624 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-24 00:00:00') TO ('2023-06-25 00:00:00');


-- gprtm.marketing_customer_log_20230625 definition

CREATE TABLE gprtm.marketing_customer_log_20230625 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-25 00:00:00') TO ('2023-06-26 00:00:00');


-- gprtm.marketing_customer_log_20230626 definition

CREATE TABLE gprtm.marketing_customer_log_20230626 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-26 00:00:00') TO ('2023-06-27 00:00:00');


-- gprtm.marketing_customer_log_20230627 definition

CREATE TABLE gprtm.marketing_customer_log_20230627 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-27 00:00:00') TO ('2023-06-28 00:00:00');


-- gprtm.marketing_customer_log_20230628 definition

CREATE TABLE gprtm.marketing_customer_log_20230628 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-28 00:00:00') TO ('2023-06-29 00:00:00');


-- gprtm.marketing_customer_log_20230629 definition

CREATE TABLE gprtm.marketing_customer_log_20230629 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-29 00:00:00') TO ('2023-06-30 00:00:00');


-- gprtm.marketing_customer_log_20230630 definition

CREATE TABLE gprtm.marketing_customer_log_20230630 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-06-30 00:00:00') TO ('2023-07-01 00:00:00');


-- gprtm.marketing_customer_log_20230701 definition

CREATE TABLE gprtm.marketing_customer_log_20230701 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-01 00:00:00') TO ('2023-07-02 00:00:00');


-- gprtm.marketing_customer_log_20230702 definition

CREATE TABLE gprtm.marketing_customer_log_20230702 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-02 00:00:00') TO ('2023-07-03 00:00:00');


-- gprtm.marketing_customer_log_20230703 definition

CREATE TABLE gprtm.marketing_customer_log_20230703 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-03 00:00:00') TO ('2023-07-04 00:00:00');


-- gprtm.marketing_customer_log_20230704 definition

CREATE TABLE gprtm.marketing_customer_log_20230704 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-04 00:00:00') TO ('2023-07-05 00:00:00');


-- gprtm.marketing_customer_log_20230705 definition

CREATE TABLE gprtm.marketing_customer_log_20230705 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-05 00:00:00') TO ('2023-07-06 00:00:00');


-- gprtm.marketing_customer_log_20230706 definition

CREATE TABLE gprtm.marketing_customer_log_20230706 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-06 00:00:00') TO ('2023-07-07 00:00:00');


-- gprtm.marketing_customer_log_20230707 definition

CREATE TABLE gprtm.marketing_customer_log_20230707 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-07 00:00:00') TO ('2023-07-08 00:00:00');


-- gprtm.marketing_customer_log_20230708 definition

CREATE TABLE gprtm.marketing_customer_log_20230708 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-08 00:00:00') TO ('2023-07-09 00:00:00');


-- gprtm.marketing_customer_log_20230709 definition

CREATE TABLE gprtm.marketing_customer_log_20230709 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-09 00:00:00') TO ('2023-07-10 00:00:00');


-- gprtm.marketing_customer_log_20230710 definition

CREATE TABLE gprtm.marketing_customer_log_20230710 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-10 00:00:00') TO ('2023-07-11 00:00:00');


-- gprtm.marketing_customer_log_20230711 definition

CREATE TABLE gprtm.marketing_customer_log_20230711 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-11 00:00:00') TO ('2023-07-12 00:00:00');


-- gprtm.marketing_customer_log_20230712 definition

CREATE TABLE gprtm.marketing_customer_log_20230712 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-12 00:00:00') TO ('2023-07-13 00:00:00');


-- gprtm.marketing_customer_log_20230713 definition

CREATE TABLE gprtm.marketing_customer_log_20230713 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-13 00:00:00') TO ('2023-07-14 00:00:00');


-- gprtm.marketing_customer_log_20230714 definition

CREATE TABLE gprtm.marketing_customer_log_20230714 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-14 00:00:00') TO ('2023-07-15 00:00:00');


-- gprtm.marketing_customer_log_20230715 definition

CREATE TABLE gprtm.marketing_customer_log_20230715 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-15 00:00:00') TO ('2023-07-16 00:00:00');


-- gprtm.marketing_customer_log_20230716 definition

CREATE TABLE gprtm.marketing_customer_log_20230716 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-16 00:00:00') TO ('2023-07-17 00:00:00');


-- gprtm.marketing_customer_log_20230717 definition

CREATE TABLE gprtm.marketing_customer_log_20230717 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-17 00:00:00') TO ('2023-07-18 00:00:00');


-- gprtm.marketing_customer_log_20230718 definition

CREATE TABLE gprtm.marketing_customer_log_20230718 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-18 00:00:00') TO ('2023-07-19 00:00:00');


-- gprtm.marketing_customer_log_20230719 definition

CREATE TABLE gprtm.marketing_customer_log_20230719 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-19 00:00:00') TO ('2023-07-20 00:00:00');


-- gprtm.marketing_customer_log_20230720 definition

CREATE TABLE gprtm.marketing_customer_log_20230720 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-20 00:00:00') TO ('2023-07-21 00:00:00');


-- gprtm.marketing_customer_log_20230721 definition

CREATE TABLE gprtm.marketing_customer_log_20230721 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-21 00:00:00') TO ('2023-07-22 00:00:00');


-- gprtm.marketing_customer_log_20230722 definition

CREATE TABLE gprtm.marketing_customer_log_20230722 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-22 00:00:00') TO ('2023-07-23 00:00:00');


-- gprtm.marketing_customer_log_20230723 definition

CREATE TABLE gprtm.marketing_customer_log_20230723 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-23 00:00:00') TO ('2023-07-24 00:00:00');


-- gprtm.marketing_customer_log_20230724 definition

CREATE TABLE gprtm.marketing_customer_log_20230724 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-24 00:00:00') TO ('2023-07-25 00:00:00');


-- gprtm.marketing_customer_log_20230725 definition

CREATE TABLE gprtm.marketing_customer_log_20230725 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-25 00:00:00') TO ('2023-07-26 00:00:00');


-- gprtm.marketing_customer_log_20230726 definition

CREATE TABLE gprtm.marketing_customer_log_20230726 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-26 00:00:00') TO ('2023-07-27 00:00:00');


-- gprtm.marketing_customer_log_20230727 definition

CREATE TABLE gprtm.marketing_customer_log_20230727 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-27 00:00:00') TO ('2023-07-28 00:00:00');


-- gprtm.marketing_customer_log_20230728 definition

CREATE TABLE gprtm.marketing_customer_log_20230728 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-28 00:00:00') TO ('2023-07-29 00:00:00');


-- gprtm.marketing_customer_log_20230729 definition

CREATE TABLE gprtm.marketing_customer_log_20230729 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-29 00:00:00') TO ('2023-07-30 00:00:00');


-- gprtm.marketing_customer_log_20230730 definition

CREATE TABLE gprtm.marketing_customer_log_20230730 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-30 00:00:00') TO ('2023-07-31 00:00:00');


-- gprtm.marketing_customer_log_20230731 definition

CREATE TABLE gprtm.marketing_customer_log_20230731 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-07-31 00:00:00') TO ('2023-08-01 00:00:00');


-- gprtm.marketing_customer_log_20230801 definition

CREATE TABLE gprtm.marketing_customer_log_20230801 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-01 00:00:00') TO ('2023-08-02 00:00:00');


-- gprtm.marketing_customer_log_20230802 definition

CREATE TABLE gprtm.marketing_customer_log_20230802 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-02 00:00:00') TO ('2023-08-03 00:00:00');


-- gprtm.marketing_customer_log_20230803 definition

CREATE TABLE gprtm.marketing_customer_log_20230803 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-03 00:00:00') TO ('2023-08-04 00:00:00');


-- gprtm.marketing_customer_log_20230804 definition

CREATE TABLE gprtm.marketing_customer_log_20230804 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-04 00:00:00') TO ('2023-08-05 00:00:00');


-- gprtm.marketing_customer_log_20230805 definition

CREATE TABLE gprtm.marketing_customer_log_20230805 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-05 00:00:00') TO ('2023-08-06 00:00:00');


-- gprtm.marketing_customer_log_20230806 definition

CREATE TABLE gprtm.marketing_customer_log_20230806 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-06 00:00:00') TO ('2023-08-07 00:00:00');


-- gprtm.marketing_customer_log_20230807 definition

CREATE TABLE gprtm.marketing_customer_log_20230807 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-07 00:00:00') TO ('2023-08-08 00:00:00');


-- gprtm.marketing_customer_log_20230808 definition

CREATE TABLE gprtm.marketing_customer_log_20230808 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-08 00:00:00') TO ('2023-08-09 00:00:00');


-- gprtm.marketing_customer_log_20230809 definition

CREATE TABLE gprtm.marketing_customer_log_20230809 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-09 00:00:00') TO ('2023-08-10 00:00:00');


-- gprtm.marketing_customer_log_20230810 definition

CREATE TABLE gprtm.marketing_customer_log_20230810 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-10 00:00:00') TO ('2023-08-11 00:00:00');


-- gprtm.marketing_customer_log_20230811 definition

CREATE TABLE gprtm.marketing_customer_log_20230811 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-11 00:00:00') TO ('2023-08-12 00:00:00');


-- gprtm.marketing_customer_log_20230812 definition

CREATE TABLE gprtm.marketing_customer_log_20230812 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-12 00:00:00') TO ('2023-08-13 00:00:00');


-- gprtm.marketing_customer_log_20230813 definition

CREATE TABLE gprtm.marketing_customer_log_20230813 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-13 00:00:00') TO ('2023-08-14 00:00:00');


-- gprtm.marketing_customer_log_20230814 definition

CREATE TABLE gprtm.marketing_customer_log_20230814 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-14 00:00:00') TO ('2023-08-15 00:00:00');


-- gprtm.marketing_customer_log_20230815 definition

CREATE TABLE gprtm.marketing_customer_log_20230815 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-15 00:00:00') TO ('2023-08-16 00:00:00');


-- gprtm.marketing_customer_log_20230816 definition

CREATE TABLE gprtm.marketing_customer_log_20230816 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-16 00:00:00') TO ('2023-08-17 00:00:00');


-- gprtm.marketing_customer_log_20230817 definition

CREATE TABLE gprtm.marketing_customer_log_20230817 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-17 00:00:00') TO ('2023-08-18 00:00:00');


-- gprtm.marketing_customer_log_20230818 definition

CREATE TABLE gprtm.marketing_customer_log_20230818 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-18 00:00:00') TO ('2023-08-19 00:00:00');


-- gprtm.marketing_customer_log_20230819 definition

CREATE TABLE gprtm.marketing_customer_log_20230819 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-19 00:00:00') TO ('2023-08-20 00:00:00');


-- gprtm.marketing_customer_log_20230820 definition

CREATE TABLE gprtm.marketing_customer_log_20230820 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-20 00:00:00') TO ('2023-08-21 00:00:00');


-- gprtm.marketing_customer_log_20230821 definition

CREATE TABLE gprtm.marketing_customer_log_20230821 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-21 00:00:00') TO ('2023-08-22 00:00:00');


-- gprtm.marketing_customer_log_20230822 definition

CREATE TABLE gprtm.marketing_customer_log_20230822 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-22 00:00:00') TO ('2023-08-23 00:00:00');


-- gprtm.marketing_customer_log_20230823 definition

CREATE TABLE gprtm.marketing_customer_log_20230823 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-23 00:00:00') TO ('2023-08-24 00:00:00');


-- gprtm.marketing_customer_log_20230824 definition

CREATE TABLE gprtm.marketing_customer_log_20230824 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-24 00:00:00') TO ('2023-08-25 00:00:00');


-- gprtm.marketing_customer_log_20230825 definition

CREATE TABLE gprtm.marketing_customer_log_20230825 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-25 00:00:00') TO ('2023-08-26 00:00:00');


-- gprtm.marketing_customer_log_20230826 definition

CREATE TABLE gprtm.marketing_customer_log_20230826 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-26 00:00:00') TO ('2023-08-27 00:00:00');


-- gprtm.marketing_customer_log_20230827 definition

CREATE TABLE gprtm.marketing_customer_log_20230827 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-27 00:00:00') TO ('2023-08-28 00:00:00');


-- gprtm.marketing_customer_log_20230828 definition

CREATE TABLE gprtm.marketing_customer_log_20230828 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-28 00:00:00') TO ('2023-08-29 00:00:00');


-- gprtm.marketing_customer_log_20230829 definition

CREATE TABLE gprtm.marketing_customer_log_20230829 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-29 00:00:00') TO ('2023-08-30 00:00:00');


-- gprtm.marketing_customer_log_20230830 definition

CREATE TABLE gprtm.marketing_customer_log_20230830 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-30 00:00:00') TO ('2023-08-31 00:00:00');


-- gprtm.marketing_customer_log_20230831 definition

CREATE TABLE gprtm.marketing_customer_log_20230831 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-08-31 00:00:00') TO ('2023-09-01 00:00:00');


-- gprtm.marketing_customer_log_20230901 definition

CREATE TABLE gprtm.marketing_customer_log_20230901 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-01 00:00:00') TO ('2023-09-02 00:00:00');


-- gprtm.marketing_customer_log_20230902 definition

CREATE TABLE gprtm.marketing_customer_log_20230902 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-02 00:00:00') TO ('2023-09-03 00:00:00');


-- gprtm.marketing_customer_log_20230903 definition

CREATE TABLE gprtm.marketing_customer_log_20230903 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-03 00:00:00') TO ('2023-09-04 00:00:00');


-- gprtm.marketing_customer_log_20230904 definition

CREATE TABLE gprtm.marketing_customer_log_20230904 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-04 00:00:00') TO ('2023-09-05 00:00:00');


-- gprtm.marketing_customer_log_20230905 definition

CREATE TABLE gprtm.marketing_customer_log_20230905 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-05 00:00:00') TO ('2023-09-06 00:00:00');


-- gprtm.marketing_customer_log_20230906 definition

CREATE TABLE gprtm.marketing_customer_log_20230906 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-06 00:00:00') TO ('2023-09-07 00:00:00');


-- gprtm.marketing_customer_log_20230907 definition

CREATE TABLE gprtm.marketing_customer_log_20230907 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-07 00:00:00') TO ('2023-09-08 00:00:00');


-- gprtm.marketing_customer_log_20230908 definition

CREATE TABLE gprtm.marketing_customer_log_20230908 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-08 00:00:00') TO ('2023-09-09 00:00:00');


-- gprtm.marketing_customer_log_20230909 definition

CREATE TABLE gprtm.marketing_customer_log_20230909 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-09 00:00:00') TO ('2023-09-10 00:00:00');


-- gprtm.marketing_customer_log_20230910 definition

CREATE TABLE gprtm.marketing_customer_log_20230910 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-10 00:00:00') TO ('2023-09-11 00:00:00');


-- gprtm.marketing_customer_log_20230911 definition

CREATE TABLE gprtm.marketing_customer_log_20230911 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-11 00:00:00') TO ('2023-09-12 00:00:00');


-- gprtm.marketing_customer_log_20230912 definition

CREATE TABLE gprtm.marketing_customer_log_20230912 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-12 00:00:00') TO ('2023-09-13 00:00:00');


-- gprtm.marketing_customer_log_20230913 definition

CREATE TABLE gprtm.marketing_customer_log_20230913 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-13 00:00:00') TO ('2023-09-14 00:00:00');


-- gprtm.marketing_customer_log_20230914 definition

CREATE TABLE gprtm.marketing_customer_log_20230914 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-14 00:00:00') TO ('2023-09-15 00:00:00');


-- gprtm.marketing_customer_log_20230915 definition

CREATE TABLE gprtm.marketing_customer_log_20230915 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-15 00:00:00') TO ('2023-09-16 00:00:00');


-- gprtm.marketing_customer_log_20230916 definition

CREATE TABLE gprtm.marketing_customer_log_20230916 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-16 00:00:00') TO ('2023-09-17 00:00:00');


-- gprtm.marketing_customer_log_20230917 definition

CREATE TABLE gprtm.marketing_customer_log_20230917 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-17 00:00:00') TO ('2023-09-18 00:00:00');


-- gprtm.marketing_customer_log_20230918 definition

CREATE TABLE gprtm.marketing_customer_log_20230918 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-18 00:00:00') TO ('2023-09-19 00:00:00');


-- gprtm.marketing_customer_log_20230919 definition

CREATE TABLE gprtm.marketing_customer_log_20230919 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-19 00:00:00') TO ('2023-09-20 00:00:00');


-- gprtm.marketing_customer_log_20230920 definition

CREATE TABLE gprtm.marketing_customer_log_20230920 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-20 00:00:00') TO ('2023-09-21 00:00:00');


-- gprtm.marketing_customer_log_20230921 definition

CREATE TABLE gprtm.marketing_customer_log_20230921 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-21 00:00:00') TO ('2023-09-22 00:00:00');


-- gprtm.marketing_customer_log_20230922 definition

CREATE TABLE gprtm.marketing_customer_log_20230922 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-22 00:00:00') TO ('2023-09-23 00:00:00');


-- gprtm.marketing_customer_log_20230923 definition

CREATE TABLE gprtm.marketing_customer_log_20230923 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-23 00:00:00') TO ('2023-09-24 00:00:00');


-- gprtm.marketing_customer_log_20230924 definition

CREATE TABLE gprtm.marketing_customer_log_20230924 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-24 00:00:00') TO ('2023-09-25 00:00:00');


-- gprtm.marketing_customer_log_20230925 definition

CREATE TABLE gprtm.marketing_customer_log_20230925 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-25 00:00:00') TO ('2023-09-26 00:00:00');


-- gprtm.marketing_customer_log_20230926 definition

CREATE TABLE gprtm.marketing_customer_log_20230926 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-26 00:00:00') TO ('2023-09-27 00:00:00');


-- gprtm.marketing_customer_log_20230927 definition

CREATE TABLE gprtm.marketing_customer_log_20230927 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-27 00:00:00') TO ('2023-09-28 00:00:00');


-- gprtm.marketing_customer_log_20230928 definition

CREATE TABLE gprtm.marketing_customer_log_20230928 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-28 00:00:00') TO ('2023-09-29 00:00:00');


-- gprtm.marketing_customer_log_20230929 definition

CREATE TABLE gprtm.marketing_customer_log_20230929 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-29 00:00:00') TO ('2023-09-30 00:00:00');


-- gprtm.marketing_customer_log_20230930 definition

CREATE TABLE gprtm.marketing_customer_log_20230930 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-09-30 00:00:00') TO ('2023-10-01 00:00:00');


-- gprtm.marketing_customer_log_20231001 definition

CREATE TABLE gprtm.marketing_customer_log_20231001 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-01 00:00:00') TO ('2023-10-02 00:00:00');


-- gprtm.marketing_customer_log_20231002 definition

CREATE TABLE gprtm.marketing_customer_log_20231002 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-02 00:00:00') TO ('2023-10-03 00:00:00');


-- gprtm.marketing_customer_log_20231003 definition

CREATE TABLE gprtm.marketing_customer_log_20231003 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-03 00:00:00') TO ('2023-10-04 00:00:00');


-- gprtm.marketing_customer_log_20231004 definition

CREATE TABLE gprtm.marketing_customer_log_20231004 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-04 00:00:00') TO ('2023-10-05 00:00:00');


-- gprtm.marketing_customer_log_20231005 definition

CREATE TABLE gprtm.marketing_customer_log_20231005 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-05 00:00:00') TO ('2023-10-06 00:00:00');


-- gprtm.marketing_customer_log_20231006 definition

CREATE TABLE gprtm.marketing_customer_log_20231006 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-06 00:00:00') TO ('2023-10-07 00:00:00');


-- gprtm.marketing_customer_log_20231007 definition

CREATE TABLE gprtm.marketing_customer_log_20231007 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-07 00:00:00') TO ('2023-10-08 00:00:00');


-- gprtm.marketing_customer_log_20231008 definition

CREATE TABLE gprtm.marketing_customer_log_20231008 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-08 00:00:00') TO ('2023-10-09 00:00:00');


-- gprtm.marketing_customer_log_20231009 definition

CREATE TABLE gprtm.marketing_customer_log_20231009 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-09 00:00:00') TO ('2023-10-10 00:00:00');


-- gprtm.marketing_customer_log_20231010 definition

CREATE TABLE gprtm.marketing_customer_log_20231010 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-10 00:00:00') TO ('2023-10-11 00:00:00');


-- gprtm.marketing_customer_log_20231011 definition

CREATE TABLE gprtm.marketing_customer_log_20231011 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-11 00:00:00') TO ('2023-10-12 00:00:00');


-- gprtm.marketing_customer_log_20231012 definition

CREATE TABLE gprtm.marketing_customer_log_20231012 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-12 00:00:00') TO ('2023-10-13 00:00:00');


-- gprtm.marketing_customer_log_20231013 definition

CREATE TABLE gprtm.marketing_customer_log_20231013 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-13 00:00:00') TO ('2023-10-14 00:00:00');


-- gprtm.marketing_customer_log_20231014 definition

CREATE TABLE gprtm.marketing_customer_log_20231014 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-14 00:00:00') TO ('2023-10-15 00:00:00');


-- gprtm.marketing_customer_log_20231015 definition

CREATE TABLE gprtm.marketing_customer_log_20231015 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-15 00:00:00') TO ('2023-10-16 00:00:00');


-- gprtm.marketing_customer_log_20231016 definition

CREATE TABLE gprtm.marketing_customer_log_20231016 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-16 00:00:00') TO ('2023-10-17 00:00:00');


-- gprtm.marketing_customer_log_20231017 definition

CREATE TABLE gprtm.marketing_customer_log_20231017 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-17 00:00:00') TO ('2023-10-18 00:00:00');


-- gprtm.marketing_customer_log_20231018 definition

CREATE TABLE gprtm.marketing_customer_log_20231018 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-18 00:00:00') TO ('2023-10-19 00:00:00');


-- gprtm.marketing_customer_log_20231019 definition

CREATE TABLE gprtm.marketing_customer_log_20231019 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-19 00:00:00') TO ('2023-10-20 00:00:00');


-- gprtm.marketing_customer_log_20231020 definition

CREATE TABLE gprtm.marketing_customer_log_20231020 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-20 00:00:00') TO ('2023-10-21 00:00:00');


-- gprtm.marketing_customer_log_20231021 definition

CREATE TABLE gprtm.marketing_customer_log_20231021 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-21 00:00:00') TO ('2023-10-22 00:00:00');


-- gprtm.marketing_customer_log_20231022 definition

CREATE TABLE gprtm.marketing_customer_log_20231022 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-22 00:00:00') TO ('2023-10-23 00:00:00');


-- gprtm.marketing_customer_log_20231023 definition

CREATE TABLE gprtm.marketing_customer_log_20231023 PARTITION OF gprtm.marketing_customer_log  FOR VALUES FROM ('2023-10-23 00:00:00') TO ('2023-10-24 00:00:00');


-- gprtm.rule_manager_log_20230719 definition

CREATE TABLE gprtm.rule_manager_log_20230719 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-07-19 00:00:00') TO ('2023-07-20 00:00:00');


-- gprtm.rule_manager_log_20230720 definition

CREATE TABLE gprtm.rule_manager_log_20230720 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-07-20 00:00:00') TO ('2023-07-21 00:00:00');


-- gprtm.rule_manager_log_20230721 definition

CREATE TABLE gprtm.rule_manager_log_20230721 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-07-21 00:00:00') TO ('2023-07-22 00:00:00');


-- gprtm.rule_manager_log_20230722 definition

CREATE TABLE gprtm.rule_manager_log_20230722 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-07-22 00:00:00') TO ('2023-07-23 00:00:00');


-- gprtm.rule_manager_log_20230723 definition

CREATE TABLE gprtm.rule_manager_log_20230723 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-07-23 00:00:00') TO ('2023-07-24 00:00:00');


-- gprtm.rule_manager_log_20230724 definition

CREATE TABLE gprtm.rule_manager_log_20230724 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-07-24 00:00:00') TO ('2023-07-25 00:00:00');


-- gprtm.rule_manager_log_20230725 definition

CREATE TABLE gprtm.rule_manager_log_20230725 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-07-25 00:00:00') TO ('2023-07-26 00:00:00');


-- gprtm.rule_manager_log_20230726 definition

CREATE TABLE gprtm.rule_manager_log_20230726 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-07-26 00:00:00') TO ('2023-07-27 00:00:00');


-- gprtm.rule_manager_log_20230727 definition

CREATE TABLE gprtm.rule_manager_log_20230727 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-07-27 00:00:00') TO ('2023-07-28 00:00:00');


-- gprtm.rule_manager_log_20230728 definition

CREATE TABLE gprtm.rule_manager_log_20230728 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-07-28 00:00:00') TO ('2023-07-29 00:00:00');


-- gprtm.rule_manager_log_20230729 definition

CREATE TABLE gprtm.rule_manager_log_20230729 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-07-29 00:00:00') TO ('2023-07-30 00:00:00');


-- gprtm.rule_manager_log_20230730 definition

CREATE TABLE gprtm.rule_manager_log_20230730 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-07-30 00:00:00') TO ('2023-07-31 00:00:00');


-- gprtm.rule_manager_log_20230731 definition

CREATE TABLE gprtm.rule_manager_log_20230731 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-07-31 00:00:00') TO ('2023-08-01 00:00:00');


-- gprtm.rule_manager_log_20230801 definition

CREATE TABLE gprtm.rule_manager_log_20230801 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-01 00:00:00') TO ('2023-08-02 00:00:00');


-- gprtm.rule_manager_log_20230802 definition

CREATE TABLE gprtm.rule_manager_log_20230802 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-02 00:00:00') TO ('2023-08-03 00:00:00');


-- gprtm.rule_manager_log_20230803 definition

CREATE TABLE gprtm.rule_manager_log_20230803 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-03 00:00:00') TO ('2023-08-04 00:00:00');


-- gprtm.rule_manager_log_20230804 definition

CREATE TABLE gprtm.rule_manager_log_20230804 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-04 00:00:00') TO ('2023-08-05 00:00:00');


-- gprtm.rule_manager_log_20230805 definition

CREATE TABLE gprtm.rule_manager_log_20230805 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-05 00:00:00') TO ('2023-08-06 00:00:00');


-- gprtm.rule_manager_log_20230806 definition

CREATE TABLE gprtm.rule_manager_log_20230806 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-06 00:00:00') TO ('2023-08-07 00:00:00');


-- gprtm.rule_manager_log_20230807 definition

CREATE TABLE gprtm.rule_manager_log_20230807 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-07 00:00:00') TO ('2023-08-08 00:00:00');


-- gprtm.rule_manager_log_20230808 definition

CREATE TABLE gprtm.rule_manager_log_20230808 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-08 00:00:00') TO ('2023-08-09 00:00:00');


-- gprtm.rule_manager_log_20230809 definition

CREATE TABLE gprtm.rule_manager_log_20230809 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-09 00:00:00') TO ('2023-08-10 00:00:00');


-- gprtm.rule_manager_log_20230810 definition

CREATE TABLE gprtm.rule_manager_log_20230810 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-10 00:00:00') TO ('2023-08-11 00:00:00');


-- gprtm.rule_manager_log_20230811 definition

CREATE TABLE gprtm.rule_manager_log_20230811 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-11 00:00:00') TO ('2023-08-12 00:00:00');


-- gprtm.rule_manager_log_20230812 definition

CREATE TABLE gprtm.rule_manager_log_20230812 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-12 00:00:00') TO ('2023-08-13 00:00:00');


-- gprtm.rule_manager_log_20230813 definition

CREATE TABLE gprtm.rule_manager_log_20230813 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-13 00:00:00') TO ('2023-08-14 00:00:00');


-- gprtm.rule_manager_log_20230814 definition

CREATE TABLE gprtm.rule_manager_log_20230814 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-14 00:00:00') TO ('2023-08-15 00:00:00');


-- gprtm.rule_manager_log_20230815 definition

CREATE TABLE gprtm.rule_manager_log_20230815 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-15 00:00:00') TO ('2023-08-16 00:00:00');


-- gprtm.rule_manager_log_20230816 definition

CREATE TABLE gprtm.rule_manager_log_20230816 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-16 00:00:00') TO ('2023-08-17 00:00:00');


-- gprtm.rule_manager_log_20230817 definition

CREATE TABLE gprtm.rule_manager_log_20230817 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-17 00:00:00') TO ('2023-08-18 00:00:00');


-- gprtm.rule_manager_log_20230818 definition

CREATE TABLE gprtm.rule_manager_log_20230818 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-18 00:00:00') TO ('2023-08-19 00:00:00');


-- gprtm.rule_manager_log_20230819 definition

CREATE TABLE gprtm.rule_manager_log_20230819 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-19 00:00:00') TO ('2023-08-20 00:00:00');


-- gprtm.rule_manager_log_20230820 definition

CREATE TABLE gprtm.rule_manager_log_20230820 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-20 00:00:00') TO ('2023-08-21 00:00:00');


-- gprtm.rule_manager_log_20230821 definition

CREATE TABLE gprtm.rule_manager_log_20230821 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-21 00:00:00') TO ('2023-08-22 00:00:00');


-- gprtm.rule_manager_log_20230822 definition

CREATE TABLE gprtm.rule_manager_log_20230822 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-22 00:00:00') TO ('2023-08-23 00:00:00');


-- gprtm.rule_manager_log_20230823 definition

CREATE TABLE gprtm.rule_manager_log_20230823 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-23 00:00:00') TO ('2023-08-24 00:00:00');


-- gprtm.rule_manager_log_20230824 definition

CREATE TABLE gprtm.rule_manager_log_20230824 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-24 00:00:00') TO ('2023-08-25 00:00:00');


-- gprtm.rule_manager_log_20230825 definition

CREATE TABLE gprtm.rule_manager_log_20230825 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-25 00:00:00') TO ('2023-08-26 00:00:00');


-- gprtm.rule_manager_log_20230826 definition

CREATE TABLE gprtm.rule_manager_log_20230826 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-26 00:00:00') TO ('2023-08-27 00:00:00');


-- gprtm.rule_manager_log_20230827 definition

CREATE TABLE gprtm.rule_manager_log_20230827 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-27 00:00:00') TO ('2023-08-28 00:00:00');


-- gprtm.rule_manager_log_20230828 definition

CREATE TABLE gprtm.rule_manager_log_20230828 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-28 00:00:00') TO ('2023-08-29 00:00:00');


-- gprtm.rule_manager_log_20230829 definition

CREATE TABLE gprtm.rule_manager_log_20230829 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-29 00:00:00') TO ('2023-08-30 00:00:00');


-- gprtm.rule_manager_log_20230830 definition

CREATE TABLE gprtm.rule_manager_log_20230830 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-30 00:00:00') TO ('2023-08-31 00:00:00');


-- gprtm.rule_manager_log_20230831 definition

CREATE TABLE gprtm.rule_manager_log_20230831 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-08-31 00:00:00') TO ('2023-09-01 00:00:00');


-- gprtm.rule_manager_log_20230901 definition

CREATE TABLE gprtm.rule_manager_log_20230901 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-01 00:00:00') TO ('2023-09-02 00:00:00');


-- gprtm.rule_manager_log_20230902 definition

CREATE TABLE gprtm.rule_manager_log_20230902 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-02 00:00:00') TO ('2023-09-03 00:00:00');


-- gprtm.rule_manager_log_20230903 definition

CREATE TABLE gprtm.rule_manager_log_20230903 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-03 00:00:00') TO ('2023-09-04 00:00:00');


-- gprtm.rule_manager_log_20230904 definition

CREATE TABLE gprtm.rule_manager_log_20230904 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-04 00:00:00') TO ('2023-09-05 00:00:00');


-- gprtm.rule_manager_log_20230905 definition

CREATE TABLE gprtm.rule_manager_log_20230905 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-05 00:00:00') TO ('2023-09-06 00:00:00');


-- gprtm.rule_manager_log_20230906 definition

CREATE TABLE gprtm.rule_manager_log_20230906 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-06 00:00:00') TO ('2023-09-07 00:00:00');


-- gprtm.rule_manager_log_20230907 definition

CREATE TABLE gprtm.rule_manager_log_20230907 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-07 00:00:00') TO ('2023-09-08 00:00:00');


-- gprtm.rule_manager_log_20230908 definition

CREATE TABLE gprtm.rule_manager_log_20230908 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-08 00:00:00') TO ('2023-09-09 00:00:00');


-- gprtm.rule_manager_log_20230909 definition

CREATE TABLE gprtm.rule_manager_log_20230909 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-09 00:00:00') TO ('2023-09-10 00:00:00');


-- gprtm.rule_manager_log_20230910 definition

CREATE TABLE gprtm.rule_manager_log_20230910 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-10 00:00:00') TO ('2023-09-11 00:00:00');


-- gprtm.rule_manager_log_20230911 definition

CREATE TABLE gprtm.rule_manager_log_20230911 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-11 00:00:00') TO ('2023-09-12 00:00:00');


-- gprtm.rule_manager_log_20230912 definition

CREATE TABLE gprtm.rule_manager_log_20230912 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-12 00:00:00') TO ('2023-09-13 00:00:00');


-- gprtm.rule_manager_log_20230913 definition

CREATE TABLE gprtm.rule_manager_log_20230913 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-13 00:00:00') TO ('2023-09-14 00:00:00');


-- gprtm.rule_manager_log_20230914 definition

CREATE TABLE gprtm.rule_manager_log_20230914 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-14 00:00:00') TO ('2023-09-15 00:00:00');


-- gprtm.rule_manager_log_20230915 definition

CREATE TABLE gprtm.rule_manager_log_20230915 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-15 00:00:00') TO ('2023-09-16 00:00:00');


-- gprtm.rule_manager_log_20230916 definition

CREATE TABLE gprtm.rule_manager_log_20230916 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-16 00:00:00') TO ('2023-09-17 00:00:00');


-- gprtm.rule_manager_log_20230917 definition

CREATE TABLE gprtm.rule_manager_log_20230917 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-17 00:00:00') TO ('2023-09-18 00:00:00');


-- gprtm.rule_manager_log_20230918 definition

CREATE TABLE gprtm.rule_manager_log_20230918 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-18 00:00:00') TO ('2023-09-19 00:00:00');


-- gprtm.rule_manager_log_20230919 definition

CREATE TABLE gprtm.rule_manager_log_20230919 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-19 00:00:00') TO ('2023-09-20 00:00:00');


-- gprtm.rule_manager_log_20230920 definition

CREATE TABLE gprtm.rule_manager_log_20230920 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-20 00:00:00') TO ('2023-09-21 00:00:00');


-- gprtm.rule_manager_log_20230921 definition

CREATE TABLE gprtm.rule_manager_log_20230921 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-21 00:00:00') TO ('2023-09-22 00:00:00');


-- gprtm.rule_manager_log_20230922 definition

CREATE TABLE gprtm.rule_manager_log_20230922 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-22 00:00:00') TO ('2023-09-23 00:00:00');


-- gprtm.rule_manager_log_20230923 definition

CREATE TABLE gprtm.rule_manager_log_20230923 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-23 00:00:00') TO ('2023-09-24 00:00:00');


-- gprtm.rule_manager_log_20230924 definition

CREATE TABLE gprtm.rule_manager_log_20230924 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-24 00:00:00') TO ('2023-09-25 00:00:00');


-- gprtm.rule_manager_log_20230925 definition

CREATE TABLE gprtm.rule_manager_log_20230925 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-25 00:00:00') TO ('2023-09-26 00:00:00');


-- gprtm.rule_manager_log_20230926 definition

CREATE TABLE gprtm.rule_manager_log_20230926 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-26 00:00:00') TO ('2023-09-27 00:00:00');


-- gprtm.rule_manager_log_20230927 definition

CREATE TABLE gprtm.rule_manager_log_20230927 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-27 00:00:00') TO ('2023-09-28 00:00:00');


-- gprtm.rule_manager_log_20230928 definition

CREATE TABLE gprtm.rule_manager_log_20230928 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-28 00:00:00') TO ('2023-09-29 00:00:00');


-- gprtm.rule_manager_log_20230929 definition

CREATE TABLE gprtm.rule_manager_log_20230929 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-29 00:00:00') TO ('2023-09-30 00:00:00');


-- gprtm.rule_manager_log_20230930 definition

CREATE TABLE gprtm.rule_manager_log_20230930 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-09-30 00:00:00') TO ('2023-10-01 00:00:00');


-- gprtm.rule_manager_log_20231001 definition

CREATE TABLE gprtm.rule_manager_log_20231001 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-01 00:00:00') TO ('2023-10-02 00:00:00');


-- gprtm.rule_manager_log_20231002 definition

CREATE TABLE gprtm.rule_manager_log_20231002 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-02 00:00:00') TO ('2023-10-03 00:00:00');


-- gprtm.rule_manager_log_20231003 definition

CREATE TABLE gprtm.rule_manager_log_20231003 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-03 00:00:00') TO ('2023-10-04 00:00:00');


-- gprtm.rule_manager_log_20231004 definition

CREATE TABLE gprtm.rule_manager_log_20231004 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-04 00:00:00') TO ('2023-10-05 00:00:00');


-- gprtm.rule_manager_log_20231005 definition

CREATE TABLE gprtm.rule_manager_log_20231005 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-05 00:00:00') TO ('2023-10-06 00:00:00');


-- gprtm.rule_manager_log_20231006 definition

CREATE TABLE gprtm.rule_manager_log_20231006 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-06 00:00:00') TO ('2023-10-07 00:00:00');


-- gprtm.rule_manager_log_20231007 definition

CREATE TABLE gprtm.rule_manager_log_20231007 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-07 00:00:00') TO ('2023-10-08 00:00:00');


-- gprtm.rule_manager_log_20231008 definition

CREATE TABLE gprtm.rule_manager_log_20231008 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-08 00:00:00') TO ('2023-10-09 00:00:00');


-- gprtm.rule_manager_log_20231009 definition

CREATE TABLE gprtm.rule_manager_log_20231009 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-09 00:00:00') TO ('2023-10-10 00:00:00');


-- gprtm.rule_manager_log_20231010 definition

CREATE TABLE gprtm.rule_manager_log_20231010 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-10 00:00:00') TO ('2023-10-11 00:00:00');


-- gprtm.rule_manager_log_20231011 definition

CREATE TABLE gprtm.rule_manager_log_20231011 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-11 00:00:00') TO ('2023-10-12 00:00:00');


-- gprtm.rule_manager_log_20231012 definition

CREATE TABLE gprtm.rule_manager_log_20231012 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-12 00:00:00') TO ('2023-10-13 00:00:00');


-- gprtm.rule_manager_log_20231013 definition

CREATE TABLE gprtm.rule_manager_log_20231013 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-13 00:00:00') TO ('2023-10-14 00:00:00');


-- gprtm.rule_manager_log_20231014 definition

CREATE TABLE gprtm.rule_manager_log_20231014 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-14 00:00:00') TO ('2023-10-15 00:00:00');


-- gprtm.rule_manager_log_20231015 definition

CREATE TABLE gprtm.rule_manager_log_20231015 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-15 00:00:00') TO ('2023-10-16 00:00:00');


-- gprtm.rule_manager_log_20231016 definition

CREATE TABLE gprtm.rule_manager_log_20231016 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-16 00:00:00') TO ('2023-10-17 00:00:00');


-- gprtm.rule_manager_log_20231017 definition

CREATE TABLE gprtm.rule_manager_log_20231017 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-17 00:00:00') TO ('2023-10-18 00:00:00');


-- gprtm.rule_manager_log_20231018 definition

CREATE TABLE gprtm.rule_manager_log_20231018 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-18 00:00:00') TO ('2023-10-19 00:00:00');


-- gprtm.rule_manager_log_20231019 definition

CREATE TABLE gprtm.rule_manager_log_20231019 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-19 00:00:00') TO ('2023-10-20 00:00:00');


-- gprtm.rule_manager_log_20231020 definition

CREATE TABLE gprtm.rule_manager_log_20231020 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-20 00:00:00') TO ('2023-10-21 00:00:00');


-- gprtm.rule_manager_log_20231021 definition

CREATE TABLE gprtm.rule_manager_log_20231021 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-21 00:00:00') TO ('2023-10-22 00:00:00');


-- gprtm.rule_manager_log_20231022 definition

CREATE TABLE gprtm.rule_manager_log_20231022 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-22 00:00:00') TO ('2023-10-23 00:00:00');


-- gprtm.rule_manager_log_20231023 definition

CREATE TABLE gprtm.rule_manager_log_20231023 PARTITION OF gprtm.rule_manager_log  FOR VALUES FROM ('2023-10-23 00:00:00') TO ('2023-10-24 00:00:00');


-- gprtm.task_manager_log_20230917 definition

CREATE TABLE gprtm.task_manager_log_20230917 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-09-17 00:00:00') TO ('2023-09-18 00:00:00');


-- gprtm.task_manager_log_20230918 definition

CREATE TABLE gprtm.task_manager_log_20230918 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-09-18 00:00:00') TO ('2023-09-19 00:00:00');


-- gprtm.task_manager_log_20230919 definition

CREATE TABLE gprtm.task_manager_log_20230919 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-09-19 00:00:00') TO ('2023-09-20 00:00:00');


-- gprtm.task_manager_log_20230920 definition

CREATE TABLE gprtm.task_manager_log_20230920 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-09-20 00:00:00') TO ('2023-09-21 00:00:00');


-- gprtm.task_manager_log_20230921 definition

CREATE TABLE gprtm.task_manager_log_20230921 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-09-21 00:00:00') TO ('2023-09-22 00:00:00');


-- gprtm.task_manager_log_20230922 definition

CREATE TABLE gprtm.task_manager_log_20230922 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-09-22 00:00:00') TO ('2023-09-23 00:00:00');


-- gprtm.task_manager_log_20230923 definition

CREATE TABLE gprtm.task_manager_log_20230923 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-09-23 00:00:00') TO ('2023-09-24 00:00:00');


-- gprtm.task_manager_log_20230924 definition

CREATE TABLE gprtm.task_manager_log_20230924 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-09-24 00:00:00') TO ('2023-09-25 00:00:00');


-- gprtm.task_manager_log_20230925 definition

CREATE TABLE gprtm.task_manager_log_20230925 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-09-25 00:00:00') TO ('2023-09-26 00:00:00');


-- gprtm.task_manager_log_20230926 definition

CREATE TABLE gprtm.task_manager_log_20230926 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-09-26 00:00:00') TO ('2023-09-27 00:00:00');


-- gprtm.task_manager_log_20230927 definition

CREATE TABLE gprtm.task_manager_log_20230927 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-09-27 00:00:00') TO ('2023-09-28 00:00:00');


-- gprtm.task_manager_log_20230928 definition

CREATE TABLE gprtm.task_manager_log_20230928 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-09-28 00:00:00') TO ('2023-09-29 00:00:00');


-- gprtm.task_manager_log_20230929 definition

CREATE TABLE gprtm.task_manager_log_20230929 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-09-29 00:00:00') TO ('2023-09-30 00:00:00');


-- gprtm.task_manager_log_20230930 definition

CREATE TABLE gprtm.task_manager_log_20230930 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-09-30 00:00:00') TO ('2023-10-01 00:00:00');


-- gprtm.task_manager_log_20231001 definition

CREATE TABLE gprtm.task_manager_log_20231001 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-01 00:00:00') TO ('2023-10-02 00:00:00');


-- gprtm.task_manager_log_20231002 definition

CREATE TABLE gprtm.task_manager_log_20231002 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-02 00:00:00') TO ('2023-10-03 00:00:00');


-- gprtm.task_manager_log_20231003 definition

CREATE TABLE gprtm.task_manager_log_20231003 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-03 00:00:00') TO ('2023-10-04 00:00:00');


-- gprtm.task_manager_log_20231004 definition

CREATE TABLE gprtm.task_manager_log_20231004 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-04 00:00:00') TO ('2023-10-05 00:00:00');


-- gprtm.task_manager_log_20231005 definition

CREATE TABLE gprtm.task_manager_log_20231005 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-05 00:00:00') TO ('2023-10-06 00:00:00');


-- gprtm.task_manager_log_20231006 definition

CREATE TABLE gprtm.task_manager_log_20231006 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-06 00:00:00') TO ('2023-10-07 00:00:00');


-- gprtm.task_manager_log_20231007 definition

CREATE TABLE gprtm.task_manager_log_20231007 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-07 00:00:00') TO ('2023-10-08 00:00:00');


-- gprtm.task_manager_log_20231008 definition

CREATE TABLE gprtm.task_manager_log_20231008 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-08 00:00:00') TO ('2023-10-09 00:00:00');


-- gprtm.task_manager_log_20231009 definition

CREATE TABLE gprtm.task_manager_log_20231009 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-09 00:00:00') TO ('2023-10-10 00:00:00');


-- gprtm.task_manager_log_20231010 definition

CREATE TABLE gprtm.task_manager_log_20231010 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-10 00:00:00') TO ('2023-10-11 00:00:00');


-- gprtm.task_manager_log_20231011 definition

CREATE TABLE gprtm.task_manager_log_20231011 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-11 00:00:00') TO ('2023-10-12 00:00:00');


-- gprtm.task_manager_log_20231012 definition

CREATE TABLE gprtm.task_manager_log_20231012 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-12 00:00:00') TO ('2023-10-13 00:00:00');


-- gprtm.task_manager_log_20231013 definition

CREATE TABLE gprtm.task_manager_log_20231013 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-13 00:00:00') TO ('2023-10-14 00:00:00');


-- gprtm.task_manager_log_20231014 definition

CREATE TABLE gprtm.task_manager_log_20231014 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-14 00:00:00') TO ('2023-10-15 00:00:00');


-- gprtm.task_manager_log_20231015 definition

CREATE TABLE gprtm.task_manager_log_20231015 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-15 00:00:00') TO ('2023-10-16 00:00:00');


-- gprtm.task_manager_log_20231016 definition

CREATE TABLE gprtm.task_manager_log_20231016 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-16 00:00:00') TO ('2023-10-17 00:00:00');


-- gprtm.task_manager_log_20231017 definition

CREATE TABLE gprtm.task_manager_log_20231017 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-17 00:00:00') TO ('2023-10-18 00:00:00');


-- gprtm.task_manager_log_20231018 definition

CREATE TABLE gprtm.task_manager_log_20231018 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-18 00:00:00') TO ('2023-10-19 00:00:00');


-- gprtm.task_manager_log_20231019 definition

CREATE TABLE gprtm.task_manager_log_20231019 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-19 00:00:00') TO ('2023-10-20 00:00:00');


-- gprtm.task_manager_log_20231020 definition

CREATE TABLE gprtm.task_manager_log_20231020 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-20 00:00:00') TO ('2023-10-21 00:00:00');


-- gprtm.task_manager_log_20231021 definition

CREATE TABLE gprtm.task_manager_log_20231021 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-21 00:00:00') TO ('2023-10-22 00:00:00');


-- gprtm.task_manager_log_20231022 definition

CREATE TABLE gprtm.task_manager_log_20231022 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-22 00:00:00') TO ('2023-10-23 00:00:00');


-- gprtm.task_manager_log_20231023 definition

CREATE TABLE gprtm.task_manager_log_20231023 PARTITION OF gprtm.task_manager_log  FOR VALUES FROM ('2023-10-23 00:00:00') TO ('2023-10-24 00:00:00');



CREATE OR REPLACE FUNCTION gprtm.func_data_cleansing(cleansing_type text, input_value text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    -- birth
    cleaned_birth text;
    -- email
    cleaned_email text;
    -- hp_no
    rst text;
    -- name
    cleaned_name text;
    -- Resno
    cleaned_birth_resno text;
    tempYYYY_resno text;
    -- resno_gender
    cleaned_birth_resno_birth text;
    -- Acreage Range
    acreage_text text;
    extracted_data text;
    -- age
    cleaned_birth_age text;
   	cleaned_age text;
   	-- age_range
  	cleaned_birth_age_range text;
 	cleaned_age_age_range text;

 	-- test
 	cleaned_name_test text;
begin
    IF (cleansing_type = 'birth') THEN
	    cleaned_birth := REGEXP_REPLACE(input_value, '[^0-9A-Za-z]', '', 'g');
	   	if (cleaned_birth = 'None') or (cleaned_birth ~ '[^0-9]') then
	   				cleaned_birth := null::date;
	   			return cleaned_birth;
	   	end if;
		if (substring(cleaned_birth,7,1) = '1') or (substring(cleaned_birth,7,1) = '5') then --1900년대 남자
			cleaned_birth := '19'||cleaned_birth;
		elsif (substring(cleaned_birth,7,1) = '3') or (substring(cleaned_birth,7,1) = '7') then --2000년대 남자
			cleaned_birth := '20'||cleaned_birth;
		elsif (substring(cleaned_birth,7,1) = '2') or (substring(cleaned_birth,7,1) = '4') then --1900년대 여자
			cleaned_birth := '19'||cleaned_birth;
		elsif (substring(cleaned_birth,7,1) = '4') or (substring(cleaned_birth,7,1) = '8') then --2000년대 여자
			cleaned_birth := '20'||cleaned_birth; 
		else
			cleaned_birth := '18'||cleaned_birth;
		end if;
	   	cleaned_birth := (left(cleaned_birth,8))::date;
		return cleaned_birth;
	
	elsif (cleansing_type = 'name_test') then
	-- 영어는 띄어쓰기 정제하면 안됨/ 한글은 띄어쓰기 정제 필요
	-- 이름에 한글만 들어가 있는 경우, 띄어쓰기 없에고 아래 로직 탐
		if not input_value ~ '[a-zA-Z]' then
			cleaned_name_test = regexp_replace(input_value, '\s+', ' ','g');
			cleaned_name_test = REPLACE(cleaned_name_test, ' ', ',');
			cleaned_name_test = REPLACE(cleaned_name_test, '/', ',');
		end if;
		if (position(',' in cleaned_name_test) > 0)  then
			cleaned_name_test = split_part(cleaned_name_test, ',' , 1);
			cleaned_name_test = split_part(cleaned_name_test, '(' , 1);
				-- 한글/영어 아닌경우 모두 제거
				IF cleaned_name_test ~ '[a-zA-Z가-힣]' THEN
		            cleaned_name_test := regexp_replace(cleaned_name_test, '[^a-zA-Z가-힣 ]', '', 'g');
		            -- 소문자인 경우 대문자로 치환
		        	IF cleaned_name_test ~ '[a-z]' THEN
		                cleaned_name_test := trim(upper(cleaned_name_test));
		            END IF;
		            IF cleaned_name_test ~ '[가-힣]' THEN
		                cleaned_name_test := replace(cleaned_name_test, ' ', '');
		            END IF;
		        END IF;
		else
				raise notice 'else 조건 ㄱㄱㄱㄱ';
				-- 한글/영어 아닌경우 모두 제거
				cleaned_name_test = split_part(input_value, '(' , 1);
				IF input_value ~ '[a-zA-Z가-힣]' THEN
		            cleaned_name_test := regexp_replace(cleaned_name_test, '[^a-zA-Z가-힣 ]', '', 'g');
		            -- 소문자인 경우 대문자로 치환
		        	IF cleaned_name_test ~ '[a-z]' THEN
		                cleaned_name_test := trim(upper(cleaned_name_test));
		            END IF;
		            IF cleaned_name_test ~ '[가-힣]' THEN
		                cleaned_name_test := replace(cleaned_name_test, ' ', '');
		            END IF;
		        END IF;
		end if;
    RETURN cleaned_name_test;
       
    ELSIF (cleansing_type = 'email') THEN
        --이메일에 공백 존재 시 제거
        cleaned_email := replace(input_value, ' ', '');
        --알파벳, '.' , '@' 제외 제거 (gmail 명명규칙을 기준으로 작성함)
        --cleaned_email := REGEXP_REPLACE(cleaned_email, '[^abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ@.]', '','g');
        cleaned_email := trim(lower(replace(cleaned_email, 'hometax@gsconst.co.kr', '')));
       	cleaned_email := trim(lower(replace(cleaned_email, 'hometax@gscosnt.co.kr', '')));
        IF NOT cleaned_email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN
            RETURN NULL;
        END IF;
        RETURN cleaned_email;
       
    ELSIF (cleansing_type = 'hp_no') THEN
        -- 특수문자 제거
        rst := REGEXP_REPLACE(input_value, '[^0-9]', '', 'g');
        -- 8글자일 경우 앞에 010 붙임
        IF length(rst) = 8 THEN
            rst := concat('010', rst);
            -- 8글자가 아닐 경우
        ELSE
            -- 가장 왼쪽에서 2글자가 82일 경우
            IF
            LEFT (rst,2) = '82' THEN
                -- 가장 왼쪽에서 4글자가 8210일 경우 (+82 1012345678 같은 번호일 경우)
                IF
                LEFT (rst,4) = '8210' THEN
                	-- 가장 앞의 8210을 010으로 치환
                    rst := ltrim(rst, '8210');
                    rst := concat('010', rst);
                -- 가장 왼쪽에서 5글자가 82010일 경우 (+82 01012345678 같은 번호일 경우)
                elseif LEFT (rst, 5) = '82010' THEN
                	-- 가장 앞의 82010을 010으로 치환
                	rst := ltrim(rst, '82010');
                    rst := concat('010', rst);
                -- 82로는 시작하지만 위의 규칙이 아닐 경우
                ELSE
                    -- 가장앞의 82를 제거
                    rst := ltrim(rst, '82');
                END IF;
                -- 가장 왼쪽에서 2글자가 10일 경우
            elseif LEFT (rst,2) = '10' THEN
            -- 가장 앞에 0을 붙임
            rst := concat('0', rst);
            END IF;
        END IF;
        -- 앞에서 11글자까지만 자름
        rst := substring(rst, 1, 11);
        IF length(rst) < 10 THEN
            RETURN NULL;
        END IF;
		-- RETURN rst; (01012345678 형태)
       	-- 010-1234-5678 형식으로 변환
       	return SUBSTRING(rst, 1, 3) || '-' || SUBSTRING(rst, 4, 4) || '-' || SUBSTRING(rst, 8);
       
--    ELSIF (cleansing_type = 'name') then
--        	-- 한글/영어 아닌경우 모두 제거
--	        IF input_value ~ '[a-zA-Z가-힣]' THEN
--	            cleaned_name := regexp_replace(input_value, '[^a-zA-Z가-힣 ]', '', 'g');
--	            -- 소문자인 경우 대문자로 치환
--	            IF cleaned_name ~ '[a-z]' THEN
--	                cleaned_name := trim(upper(cleaned_name));
--	            END IF;
--	            IF cleaned_name ~ '[가-힣]' THEN
--	                cleaned_name := replace(cleaned_name, ' ', '');
--	            END IF;
--	        END IF;
--    	RETURN cleaned_name;
       
       elsif (cleansing_type = 'name') then
       if not input_value ~ '[a-zA-Z]' then
			cleaned_name_test = regexp_replace(input_value, '\s+', ' ','g');
			cleaned_name_test = REPLACE(cleaned_name_test, ' ', ',');
			cleaned_name_test = REPLACE(cleaned_name_test, '/', ',');
		end if;
		if (position(',' in cleaned_name_test) > 0)  then
			cleaned_name_test = split_part(cleaned_name_test, ',' , 1);
			cleaned_name_test = split_part(cleaned_name_test, '(' , 1);
				-- 한글/영어 아닌경우 모두 제거
				IF cleaned_name_test ~ '[a-zA-Z가-힣]' THEN
		            cleaned_name_test := regexp_replace(cleaned_name_test, '[^a-zA-Z가-힣 ]', '', 'g');
		            -- 소문자인 경우 대문자로 치환
		        	IF cleaned_name_test ~ '[a-z]' THEN
		                cleaned_name_test := trim(upper(cleaned_name_test));
		            END IF;
		            IF cleaned_name_test ~ '[가-힣]' THEN
		                cleaned_name_test := replace(cleaned_name_test, ' ', '');
		            END IF;
		        END IF;
		else
				-- 한글/영어 아닌경우 모두 제거
				cleaned_name_test = split_part(input_value, '(' , 1);
				IF input_value ~ '[a-zA-Z가-힣]' THEN
		            cleaned_name_test := regexp_replace(cleaned_name_test, '[^a-zA-Z가-힣 ]', '', 'g');
		            -- 소문자인 경우 대문자로 치환
		        	IF cleaned_name_test ~ '[a-z]' THEN
		                cleaned_name_test := trim(upper(cleaned_name_test));
		            END IF;
		            IF cleaned_name_test ~ '[가-힣]' THEN
		                cleaned_name_test := replace(cleaned_name_test, ' ', '');
		            END IF;
		        END IF;
		end if;
    RETURN cleaned_name_test;
       
    ELSIF (cleansing_type = 'resno') THEN
        -- 숫자가 아닌 것은 공백으로 치환
        cleaned_birth_resno := REGEXP_REPLACE(input_value, '[^0-9]', '', 'g');
        cleaned_birth_resno :=
    LEFT (cleaned_birth_resno,
        6);
        IF length(cleaned_birth_resno) = 6 THEN
            -- 기본적으로는 19XX 년도로 가정하고 계산 (2100년대가 되면 변수 수정 필요함)
            tempYYYY_resno := '19';
            -- 현재년도 - 19XX을 계산했을 때 90보다 클 경우 (나이가 90살 이상)
            IF DATE_PART('year', now()) - concat(tempYYYY_resno,
            LEFT (cleaned_birth_resno, 2))::int > 90 THEN
                -- 20XX년으로 계
                tempYYYY_resno := '20';
            END IF;
            -- 계산한 년도가 현재 년보다 미래일 경우 다시 19XX로 수정
            IF DATE_PART('year', now()) - concat(tempYYYY_resno,
            LEFT (cleaned_birth_resno, 2))::int <= 0 THEN
                tempYYYY_resno := '19';
            END IF;
            -- 계산 완료된 년도를 cleaned_birth_resno로 사용
            cleaned_birth_resno := concat(tempYYYY_resno, cleaned_birth_resno);
            RETURN cleaned_birth_resno::date;
            -- 6자리도 8자리도 아닌 외의 경우는 NULL로 치환 (그대로 사용하기에는 timestamp형태로 변환이 불가능함)
        ELSE
            RETURN NULL;
        END IF;
       
    ELSIF (cleansing_type = 'resno_gender') THEN
        -- 숫자가 아닌 것은 공백으로 치환
        cleaned_birth_resno_birth := REGEXP_REPLACE(input_value, '[^0-9]', '', 'g');
        cleaned_birth_resno_birth :=
    substring(cleaned_birth_resno_birth,7,1);
        IF cleaned_birth_resno_birth = '1' OR cleaned_birth_resno_birth = '3' THEN
            RETURN 'M';
            elseif cleaned_birth_resno_birth = '2'
                OR cleaned_birth_resno_birth = '4' THEN
                RETURN 'W';
        ELSE
            RETURN NULL;
        END IF;
       
    ELSIF (cleansing_type = 'acreage') THEN
        extracted_data := regexp_replace(input_value, '[^0-9가-힣a-zA-Z]', '', 'g');
        IF extracted_data = '' OR extracted_data = '무응답' THEN
            RETURN '무응답';
        ELSE
            IF extracted_data::int <= 60 THEN
                acreage_text := '60㎡ 이하';
            ELSIF extracted_data::int > 60
                    AND extracted_data::int <= 85 THEN
                    acreage_text := '61㎡ ~ 85㎡';
            ELSIF extracted_data::int > 85
                    AND extracted_data::int <= 99 THEN
                    acreage_text := '86㎡ ~ 99㎡';
            ELSIF extracted_data::int > 99 THEN
                acreage_text := '100㎡ 초과';
            ELSE
                acreage_text := '범위에 없는 값 입니다';
            END IF;
            RETURN acreage_text;
        END IF;
       
    ELSIF (cleansing_type = 'age') THEN
	    cleaned_birth_age := REGEXP_REPLACE(input_value, '[^0-9]', '', 'g');
		if (right(cleaned_birth_age,1) = '1') or (right(cleaned_birth_age,1) = '5') then --1900년대 남자
			cleaned_birth_age := '19'||cleaned_birth_age;
		elsif (right(cleaned_birth_age,1) = '3') or (right(cleaned_birth_age,1) = '7') then --2000년대 남자
			cleaned_birth_age := '20'||cleaned_birth_age;
		elsif (right(cleaned_birth_age,1) = '2') or (right(cleaned_birth_age,1) = '4') then --1900년대 여자
			cleaned_birth_age := '19'||cleaned_birth_age;
		elsif (right(cleaned_birth_age,1) = '4') or (right(cleaned_birth_age,1) = '8') then --2000년대 여자
			cleaned_birth_age := '20'||cleaned_birth_age; 
		else
			cleaned_birth_age := '18'||cleaned_birth_age;
		end if;
	   	cleaned_age := extract (year from age(current_date, (left(cleaned_birth_age,8))::date))::text;
	 	return cleaned_age;

    ELSIF (cleansing_type = 'age_range') then
    	    cleaned_birth_age_range := REGEXP_REPLACE(input_value, '[^0-9]', '', 'g');
		if (right(cleaned_birth_age_range,1) = '1') or (right(cleaned_birth_age_range,1) = '5') then --1900년대 남자
			cleaned_birth_age_range := '19'||cleaned_birth_age_range;
		elsif (right(cleaned_birth_age_range,1) = '3') or (right(cleaned_birth_age_range,1) = '7') then --2000년대 남자
			cleaned_birth_age_range := '20'||cleaned_birth_age_range;
		elsif (right(cleaned_birth_age_range,1) = '2') or (right(cleaned_birth_age_range,1) = '4') then --1900년대 여자
			cleaned_birth_age_range := '19'||cleaned_birth_age_range;
		elsif (right(cleaned_birth_age_range,1) = '4') or (right(cleaned_birth_age_range,1) = '8') then --2000년대 여자
			cleaned_birth_age_range := '20'||cleaned_birth_age_range; 
		else
			cleaned_birth_age_range := '18'||cleaned_birth_age_range;
		end if;
	   	cleaned_age_age_range := extract (year from age(current_date, (left(cleaned_birth_age_range,8))::date))::text;
        IF cleaned_age_age_range::int < 20 THEN
            RETURN 10;
        ELSE
            RETURN cleaned_age_age_range::int - (cleaned_age_age_range::int % 10);
        END IF;
    else
        -- 오류 발생 시 NULL값 리턴	
        return null;
      	-- 오류 발생 시 exception 
		-- RAISE EXCEPTION 'Invalid cleansing_type: %', cleansing_type;
    END IF;
          
    EXCEPTION
    WHEN others THEN
        RETURN NULL;
END;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.func_data_cleansing_int_to_timestamp(cleansing_type text, input_value integer)
 RETURNS timestamp without time zone
 LANGUAGE plpgsql
AS $function$
declare 
	int_to_timestamp timestamp;
begin
	if (cleansing_type = 'time') then
		int_to_timestamp := TO_TIMESTAMP(input_value/1000);
		return int_to_timestamp;
	end if;
END;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.func_hc_lv0_to_task()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
declare 
	table_tp varchar;
begin	
    IF (CURRENT_USER = 'task_worker') THEN
      return New;
    END IF;
		
	IF jsonb_set(jsonb_set(jsonb_set(row_to_json(OLD)::jsonb, '{systemmodstamp}', 'null'), '{lastmodifieddate}', 'null'), '{customerexternalid__c}', 'null') =
	   jsonb_set(jsonb_set(jsonb_set(row_to_json(NEW)::jsonb, '{systemmodstamp}', 'null'), '{lastmodifieddate}', 'null'), '{customerexternalid__c}', 'null') THEN
	    RETURN new;
	END IF;
   
	if (TG_OP = 'INSERT' or TG_OP = 'UPDATE') and NEW.phone1__c like '%\_%' then
		return new;
	end if;


    EXECUTE 'SELECT table_tp FROM "gprtm"."legacy_manager" WHERE "lv0_schema_nm" = $1 AND "lv0_table_nm" = $2'
    INTO table_tp
    USING TG_TABLE_SCHEMA, TG_TABLE_NAME;
	
    if table_tp = 'I' then
	    INSERT INTO gprtm.task_manager  ("lv0_schema_nm", "lv0_table_nm", "dml_tp", "asis_json", "tobe_json","union_tf","link_tf","unified_tf")
	    VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP, row_to_json(OLD), row_to_json(NEW), false, false, false);
    elseif table_tp = 'P' then
	    INSERT INTO gprtm.task_manager ("lv0_schema_nm", "lv0_table_nm", "dml_tp", "asis_json", "tobe_json", "unified_tf")
	    VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP, row_to_json(OLD), row_to_json(NEW), false);
   else
	    INSERT INTO gprtm.task_manager ("lv0_schema_nm", "lv0_table_nm", "dml_tp", "asis_json", "tobe_json")
	    VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP, row_to_json(OLD), row_to_json(NEW));
   end if;
 
    RETURN NEW;
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error in process_change_event: % - %', SQLERRM, SQLSTATE;
        RETURN NULL;
    END;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.func_legacy_to_rule_manager()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare 
	channel_array jsonb;
	condition_array jsonb;
	condition_detail jsonb;
	
	new_source_json jsonb;
	new_condition_array jsonb[];

	new_default_json jsonb;

	v_i integer;

	BEGIN
		select jsonb_agg(channel_tp) into channel_array 
		from (select distinct channel_tp from gprtm.legacy_manager) as lm;
	
		new_source_json := jsonb_build_object('Source', channel_array);
		
		select default_json -> 'condition' into condition_array from gprtm.rule_manager rm;
			
		FOR v_i IN 0..2 loop
			select default_json -> 'condition' -> v_i into condition_detail from gprtm.rule_manager rm;
			
			raise notice '%' , condition_detail;
		END LOOP;
	
		return true;
	END;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.func_lv0_to_task()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
declare 
	table_tp varchar;
begin	
    IF (CURRENT_USER = 'task_worker') THEN
      return New;
    END IF;
	
    IF row_to_json(OLD)::jsonb = row_to_json(NEW)::jsonb THEN
        RETURN NEW;
    END IF;
   
    EXECUTE 'SELECT table_tp FROM "gprtm"."legacy_manager" WHERE "lv0_schema_nm" = $1 AND "lv0_table_nm" = $2'
    INTO table_tp
    USING TG_TABLE_SCHEMA, TG_TABLE_NAME;
	
    if table_tp = 'I' then
	    INSERT INTO gprtm.task_manager  ("lv0_schema_nm", "lv0_table_nm", "dml_tp", "asis_json", "tobe_json","union_tf","link_tf","unified_tf")
	    VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP, row_to_json(OLD), row_to_json(NEW), false, false, false);
    elseif table_tp = 'P' then
	    INSERT INTO gprtm.task_manager ("lv0_schema_nm", "lv0_table_nm", "dml_tp", "asis_json", "tobe_json", "unified_tf")
	    VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP, row_to_json(OLD), row_to_json(NEW), false);
   else
	    INSERT INTO gprtm.task_manager ("lv0_schema_nm", "lv0_table_nm", "dml_tp", "asis_json", "tobe_json")
	    VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP, row_to_json(OLD), row_to_json(NEW));
   end if;
 
    RETURN NEW;
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error in process_change_event: % - %', SQLERRM, SQLSTATE;
        RETURN NULL;
    END;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.func_lv0_to_task_manual(schema_nm character varying, table_nm character varying)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare 
	table_tp varchar;
begin	
    EXECUTE 'SELECT table_tp FROM "gprtm"."legacy_manager" WHERE "lv0_schema_nm" = $1 AND "lv0_table_nm" = $2'
    INTO table_tp
    USING schema_nm, table_nm;
	
    IF table_tp = 'I' THEN
        EXECUTE 'INSERT INTO gprtm.task_manager ("lv0_schema_nm", "lv0_table_nm", "dml_tp", "asis_json", "tobe_json","union_tf","link_tf","unified_tf")
                SELECT $1, $2, ''INSERT'', null, row_to_json(A), false, false, false FROM "' || schema_nm || '"."' || table_nm || '" A' 
        USING schema_nm, table_nm;
    ELSIF table_tp = 'P' THEN
        EXECUTE 'INSERT INTO gprtm.task_manager ("lv0_schema_nm", "lv0_table_nm", "dml_tp", "asis_json", "tobe_json","unified_tf")
                SELECT $1, $2, ''INSERT'', null, row_to_json(A), false FROM "' || schema_nm || '"."' || table_nm || '" A' 
        USING schema_nm, table_nm;
    ELSE
        EXECUTE 'INSERT INTO gprtm.task_manager ("lv0_schema_nm", "lv0_table_nm", "dml_tp", "asis_json", "tobe_json")
                SELECT $1, $2, ''INSERT'', null, row_to_json(A) FROM "' || schema_nm || '"."' || table_nm || '" A' 
        USING schema_nm, table_nm;
    END IF;
 
 
    RETURN TRUE;
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error in process_change_event: % - %', SQLERRM, SQLSTATE;
        RETURN FALSE;
    END;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.func_lv1_to_union(seq_param integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    row_json json;
    common_fields text;
    common_field_array text[];
    common_field_length int;
   	common_field_type_array text[];
    new_mbr_seq text;
   	target_schema text;
   	target_table text;
    v_table_name text;
    profile_record RECORD;
   
    insert_query text;
    delete_query text;
    update_query text;
   
    cnt int := 1;
   	special_tf int := 1;
   
    channel_code text;
    temp_text text;
   
    text1 text;
    text2 text;
    text3 text;
--   	return_text text;
begin
    raise notice 'L1 -> UNION Start';

    select * into profile_record from gprtm.task_manager where seq = seq_param;

   	select lv1_schema_nm, lv1_table_nm, channel_tp into target_schema, target_table, channel_code from gprtm.legacy_manager where lv0_schema_nm = profile_record.lv0_schema_nm and lv0_table_nm = profile_record.lv0_table_nm;
   	
   
   	raise notice '%, %, %', target_schema, target_table, channel_code;
   -- 동적 SQL을 위해 테이블 이름 생성
    v_table_name := '"' || target_schema || '".' || '"' || target_table || '"' ;
   -- 공통 필드 목록 추출
    select replace(quote_ident(ARRAY_To_string(
        ARRAY(
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = target_table AND table_schema = target_schema AND
                  column_name IN (
                      SELECT column_name
                      FROM information_schema.columns
                      WHERE table_name = 'union_customer' AND
                            column_name <> 'SEQ'
                  ) 
        ), ','
    )),',','","')
    INTO common_fields;
   
   
    if profile_record.dml_tp = 'INSERT' then
    	raise notice 'INSERT';
        
        new_mbr_seq := profile_record.cust_seq;
       
       	select count(*) into special_tf from gprtm.union_customer where channel_tp = channel_code and cust_seq = new_mbr_seq limit 1;
       	-- 같은 channel_code, cust_seq 를 가진 union 레코드가 존재한다면 delete 부
    	if special_tf >= 1 then
    		-- DELETE 쿼리 생성
	        delete_query := 'DELETE FROM gprtm.union_customer' || ' WHERE cust_seq=' || '''' || new_mbr_seq || '''' || '::varchar' || ' and ' || 'channel_tp=' || '''' || channel_code || '''' ||';';
	        
	        -- 동적 SQL 실행
	        EXECUTE delete_query;
    	end if;
      
        -- INSERT 쿼리 생성
        insert_query := 'INSERT INTO gprtm.union_customer '|| '(' || common_fields || ',' || 'channel_tp) ' ||
                        'SELECT ' || common_fields || ',' || '''' || channel_code  || '''' || ' FROM ' || v_table_name || ' WHERE cust_seq = ' || '''' || new_mbr_seq || '''' || '::varchar' ||';';
        
--      raise notice '%',"insert_query";
        -- 동적 SQL 실행
        EXECUTE insert_query;
        
    elsif profile_record.dml_tp = 'DELETE' then
        raise notice 'DELETE';
        
        new_mbr_seq := profile_record.cust_seq;
       
       	raise notice '%, %',channel_code, new_mbr_seq;
    
        -- DELETE 쿼리 생성
        delete_query := 'DELETE FROM gprtm.union_customer' || ' WHERE cust_seq=' || '''' || new_mbr_seq || '''' || '::varchar' || ' and ' || 'channel_tp=' || '''' || channel_code || '''' ||';';
        
        -- 동적 SQL 실행
        EXECUTE delete_query;
    
    else -- 'UPDATE'
        raise notice 'UPDATE';
        
        new_mbr_seq := profile_record.cust_seq;
       
       	select count(*) into special_tf from gprtm.union_customer where channel_tp = channel_code and cust_seq = new_mbr_seq limit 1;
    	
       	-- 같은 channel_code, cust_seq를 가진 union 레코드가 존재한다면 update 로직
       	if special_tf >= 1 then
    		-- L1 - UNION 사이의 공통필드배열 생성
	        select ARRAY(
	            SELECT column_name
	            FROM information_schema.columns
	            WHERE table_name = target_table and table_schema = target_schema and
	                  column_name IN (
	                      SELECT column_name
	                      FROM information_schema.columns
	                      WHERE table_name = 'union_customer' AND
	                            column_name <> 'SEQ'
	                  ) 
	        ) into common_field_array;
	       
	   
	        select array_length(ARRAY(
	            SELECT column_name
	            FROM information_schema.columns
	            WHERE table_name = target_table and table_schema = target_schema and
	                  column_name IN (
	                      SELECT column_name
	                      FROM information_schema.columns
	                      WHERE table_name = 'union_customer' AND
	                            column_name <> 'SEQ'
	                  ) 
	        ),1) into common_field_length;
	       
	       	
	       	select array(
				select data_type  
				from information_schema.columns
				where 1=1
				and table_name = target_table and table_schema = target_schema and
				column_name 
				in
				(
					select column_name
					from information_schema.columns
					where 1=1
					and table_name = 'union_customer'
					and column_name != 'SEQ'
				
				)
			)
			into common_field_type_array;
	       
	       
	        execute 'select row_to_json(t) from (SELECT ' || common_fields || 'FROM ' || v_table_name || ' WHERE cust_seq'  || '=' || '''' || new_mbr_seq || '''' || '::varchar' || ') as t' 
	        into row_json; 
	       	
	       	raise notice '%',common_field_array;
	       
	       	raise notice '%',common_field_length;
	       
	       	raise notice '%',row_json;
	       
	       
	        -- UPDATE 쿼리 생성
	        update_query := 'UPDATE gprtm.union_customer SET ';
	        
	        loop
	            cnt := cnt+1;
	            exit when cnt > common_field_length;
	            temp_text := row_json->>common_field_array[cnt];
	           
	           	raise notice '%',temp_text;
	            if temp_text is null then update_query := update_query || '"' ||common_field_array[cnt] || '"' || '=' || 'null' || ',';
				else
					if common_field_type_array[cnt] = 'character varying' then 
						temp_text := replace(temp_text ,'''', '''''');
					end if;
					update_query := update_query || '"' ||common_field_array[cnt] || '"' || '=' || '''' ||temp_text || '''' || '::' ||common_field_type_array[cnt] ||','; 
				end if;
	           	
	    		raise notice '%',update_query;
	        end loop;
	       
	        update_query := substring(update_query, 1, length(update_query)-1);
	        update_query := update_query || ' where cust_seq=' || '''' || new_mbr_seq || '''' || '::varchar' || ' and channel_tp=' || '''' || channel_code || '''' || ';';
	        
	       	raise notice '%',update_query;
	       
	--       동적 SQL 실행
	        EXECUTE update_query;
	       
	    -- 같은 channel_code, cust_seq 가진 union 레코드가 존재한다면 update 
	    else 
	    	-- INSERT 쿼리 생성
	        insert_query := 'INSERT INTO gprtm.union_customer '|| '(' || common_fields || ',' || 'channel_tp) ' ||
	                        'SELECT ' || common_fields || ',' || '''' || channel_code  || '''' || ' FROM ' || v_table_name || ' WHERE cust_seq = ' || '''' || new_mbr_seq || '''' || '::varchar' ||';';
	        
	--      raise notice '%',"insert_query";
	        -- 동적 SQL 실행
	        EXECUTE insert_query;
    	end if;
       
    end if;
   
    if profile_record.error_tf = true then
		UPDATE gprtm.task_manager
	   	SET 
	   		union_tf = true,
	   		error_tf = false,
	   		error_retry_cnt = 0
	    WHERE seq = seq_param;
	else 
		UPDATE gprtm.task_manager
		SET union_tf = true WHERE seq = seq_param;
	END IF;
   

    raise notice 'L1 -> UNION End';
	return true;
   
    exception WHEN OTHERS then
   	   	get STACKED diagnostics
	        text1 = RETURNed_SQLSTATE,
	        text2 = MESSAGE_TEXT,
	        text3 = PG_EXCEPTION_CONTEXT;
	   
	    raise notice 'returned_sqlstate = %',text1;
	    raise notice 'message_text = %',text2;
	    raise notice 'pg_exception_context = %',text3;
	   	
	   	return false;
--	   	text := 'L1 -> Union 실패' profile_record.lv0_schema_nm, profile_record.lv0_table_nm;
    
    
END;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.func_marketing_to_log()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
	IF TG_OP = 'INSERT' THEN
	    INSERT INTO gprtm.marketing_customer_log (dml_tp, seq, channel_tp, cust_seq, cust_marketing_tp, cust_marketing_tf, cust_modify_dt, register_dt, modify_dt)
	    VALUES ('INSERT', NEW.seq, NEW.channel_tp, NEW.cust_seq, NEW.cust_marketing_tp, NEW.cust_marketing_tf, NEW.cust_modify_dt, NEW.register_dt, NEW.modify_dt);
	ELSIF TG_OP = 'UPDATE' THEN
	    INSERT INTO gprtm.marketing_customer_log (dml_tp, seq, channel_tp, cust_seq, cust_marketing_tp, cust_marketing_tf, cust_modify_dt, register_dt, modify_dt)
	    VALUES ('UPDATE', NEW.seq, NEW.channel_tp, NEW.cust_seq, NEW.cust_marketing_tp, NEW.cust_marketing_tf, NEW.cust_modify_dt, NEW.register_dt, NEW.modify_dt);
	ELSIF TG_OP = 'DELETE' THEN
	    INSERT INTO gprtm.marketing_customer_log (dml_tp, seq, channel_tp, cust_seq, cust_marketing_tp, cust_marketing_tf, cust_modify_dt, register_dt, modify_dt)
	    VALUES ('DELETE', OLD.seq, OLD.channel_tp, OLD.cust_seq, OLD.cust_marketing_tp, OLD.cust_marketing_tf, OLD.cust_modify_dt, OLD.register_dt, timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP));
	END IF;
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.func_return_uuid(channel_param character varying, seq_param character varying, name_param character varying, email_param character varying, phone_param character varying)
 RETURNS character varying[]
 LANGUAGE plpgsql
AS $function$
declare 
	return_uuid _varchar;
	asis_uuid varchar;
	uuid_cnt int;
begin 
	
	return_uuid := array(
		select distinct a.uuid
		from 
		(
			select a.uuid, a.register_dt  
			from gprtm.link_customer as a
			join 
			(
				-- (이름+전화번호) or (이름+이메일) or (전화번호+이메일) 로 묶였다는 것은 uuid 가 고유한 1개의 값으로 통합되었어야만 함. 
				-- (CHANNEL_CODE, MBR_SEQ) N : UUID 1
				select channel_tp, cust_seq , register_dt 
				from gprtm.union_customer 
				where 1=1
				and not (
					(channel_tp,cust_seq) in
					(select lm.channel_tp,tm.cust_seq from gprtm.task_manager tm 
					join gprtm.legacy_manager lm on lm.lv0_schema_nm =tm.lv0_schema_nm and lm.lv0_table_nm = tm.lv0_table_nm 
					where union_tf = true and link_tf = false)
				)
				and (
					(cust_nm = name_param and cust_phone = phone_param)
					or (cust_nm = name_param and cust_email = email_param)
					or (cust_phone = phone_param and cust_email = email_param)
				)
			) as b
			ON(a.channel_tp = b.channel_tp and a.cust_seq = b.cust_seq)
			where 1=1
			order by a.register_dt 
		) as a
	);

	raise notice '%',return_uuid[1];
	
	if return_uuid[1] is null then 
		select uuid into asis_uuid
		from gprtm.link_customer
		where 1=1
		and channel_tp = channel_param
		and cust_seq = seq_param;
	
		select count(*) into uuid_cnt
		from gprtm.link_customer
		where 1=1
		and uuid = asis_uuid;
	
		if asis_uuid is null or uuid_cnt > 1 then raise notice '%',array[encode(gen_random_bytes(16), 'hex')]; return array[encode(gen_random_bytes(16), 'hex')];
		else raise notice '%',array[asis_uuid]; return array[asis_uuid];
		end if;
	
	else return return_uuid;
	
	end if;
	
end;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.func_rule_to_log()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
	IF TG_OP = 'INSERT' THEN
	    INSERT INTO gprtm.rule_manager_log (task_dml_tp, seq, rule_json, default_json, condition_json, register_dt, modify_dt)
	    VALUES ('INSERT', NEW.seq, NEW.rule_json, NEW.default_json, new.condition_json, NEW.register_dt, NEW.modify_dt);
	ELSIF TG_OP = 'UPDATE' THEN
	    INSERT INTO gprtm.rule_manager_log (task_dml_tp, seq, rule_json, default_json, condition_json, register_dt, modify_dt)
	    VALUES ('UPDATE', NEW.seq, NEW.rule_json, NEW.default_json, new.condition_json, NEW.register_dt, NEW.modify_dt);
	ELSIF TG_OP = 'DELETE' THEN
	    INSERT INTO gprtm.rule_manager_log (task_dml_tp, seq, rule_json, default_json, condition_json, register_dt, modify_dt)
	    VALUES ('DELETE', OLD.seq, OLD.rule_json, OLD.default_json, OLD.register_dt, old.condition_json, timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP));
	END IF;
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.func_search_union_insert(name_param character varying, phone_param character varying, email_param character varying)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
declare 
	return_uuids varchar[];
	temp_json jsonb;
	json_return jsonb := '{}'::jsonb;
begin 
	
	select array(
		select distinct a.uuid
		from gprtm.link_customer as a
		join 
		(
			-- 자기 자신 빼고 통합되는 uuid 전부 search
			select channel_tp, cust_seq 
			from gprtm.union_customer 
			where 1=1
			and not (
				(channel_tp,cust_seq) in
				(select lm.channel_tp,tm.cust_seq from gprtm.task_manager tm 
				join gprtm.legacy_manager lm on lm.lv0_schema_nm =tm.lv0_schema_nm and lm.lv0_table_nm = tm.lv0_table_nm 
				where union_tf = true and link_tf = false)
			)
			and (
				(cust_nm = name_param and cust_phone = phone_param)
				or (cust_nm = name_param and cust_email = email_param)
				or (cust_phone = phone_param and cust_email = email_param)
			)
		) as b
		ON(a.channel_tp = b.channel_tp and a.cust_seq = b.cust_seq)
		where 1=1
	)
	into return_uuids;
	

	temp_json := to_jsonb(return_uuids);

--	raise notice '%', temp_json;

	json_return := jsonb_set(json_return, '{related_uuids}', temp_json);

--	raise notice '%', json_return;

	-- 자기 자신만 통합되었음
	if array_length(return_uuids,1) is null then
		raise notice '0개';
		temp_json := to_jsonb(encode(gen_random_bytes(16), 'hex'));
		raise notice '%', temp_json;
		json_return := jsonb_set(json_return, '{uuid}', temp_json);
		raise notice '%', json_return;
		return json_return;
	-- 자기 말고 또 다른 통합된 레코드(자신 포함 2개의 uuid)가 존재
	elsif array_length(return_uuids,1) = 1 then
		-- 발견된 uuid 그대로 따라감
		raise notice '1개';
		temp_json := to_jsonb(return_uuids[1]);
		raise notice '%', temp_json;
		json_return := jsonb_set(json_return, '{uuid}', temp_json);
		raise notice '%', json_return;
		return json_return;
	-- 자기 말고 또 다른 통합된 레코드(자신 포함 2개 이상의 uuid)들이 존재
	else
		-- 발견한 uuids 중 랜덤? 으로 첫 uuid 반환
		raise notice 'N개';
		temp_json := to_jsonb(return_uuids[1]);
		raise notice '%', temp_json;
		json_return := jsonb_set(json_return, '{uuid}', temp_json);
		raise notice '%', json_return;
		return json_return;
	end if;
end;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.func_task_to_log()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
	IF TG_OP = 'INSERT' THEN
	    INSERT INTO gprtm.task_manager_log (task_dml_tp, seq, dml_tp, lv0_schema_nm, lv0_table_nm, cust_seq, asis_json, tobe_json, lv1_tf, lv1_external_tf, union_tf, link_tf, unified_tf, lv2_tf, error_tf, error_retry_cnt, effected_uuid, changed_effected_uuid, register_dt, modify_dt)
	    VALUES ('INSERT', NEW.seq, NEW.dml_tp, NEW.lv0_schema_nm, NEW.lv0_table_nm, NEW.cust_seq, NEW.asis_json, NEW.tobe_json, NEW.lv1_tf, NEW.lv1_external_tf, NEW.union_tf, NEW.link_tf, NEW.unified_tf, NEW.lv2_tf, NEW.error_tf, NEW.error_retry_cnt, NEW.effected_uuid, new.changed_effected_uuid, NEW.register_dt, NEW.modify_dt);
	ELSIF TG_OP = 'UPDATE' THEN
	    INSERT INTO gprtm.task_manager_log (task_dml_tp, seq, dml_tp, lv0_schema_nm, lv0_table_nm, cust_seq, asis_json, tobe_json, lv1_tf, lv1_external_tf, union_tf, link_tf, unified_tf, lv2_tf, error_tf, error_retry_cnt, effected_uuid, changed_effected_uuid, register_dt, modify_dt)
	    VALUES ('UPDATE', NEW.seq, NEW.dml_tp, NEW.lv0_schema_nm, NEW.lv0_table_nm, NEW.cust_seq, OLD.asis_json, NEW.tobe_json, NEW.lv1_tf, NEW.lv1_external_tf, NEW.union_tf, NEW.link_tf, NEW.unified_tf, NEW.lv2_tf, NEW.error_tf, NEW.error_retry_cnt, NEW.effected_uuid, new.changed_effected_uuid, NEW.register_dt, NEW.modify_dt);
	ELSIF TG_OP = 'DELETE' THEN
	    INSERT INTO gprtm.task_manager_log (task_dml_tp, seq, dml_tp, lv0_schema_nm, lv0_table_nm, cust_seq, asis_json, tobe_json, lv1_tf, lv1_external_tf, union_tf, link_tf, unified_tf, lv2_tf, error_tf, error_retry_cnt, effected_uuid, changed_effected_uuid, register_dt, modify_dt)
	    VALUES ('DELETE', OLD.seq, OLD.dml_tp, OLD.lv0_schema_nm, OLD.lv0_table_nm, OLD.cust_seq, OLD.asis_json, OLD.tobe_json, OLD.lv1_tf, OLD.lv1_external_tf, OLD.union_tf, OLD.link_tf, OLD.unified_tf, OLD.lv2_tf, OLD.error_tf, OLD.error_retry_cnt, OLD.effected_uuid, old.changed_effected_uuid, OLD.register_dt, timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP));
	END IF;
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.func_testing(seq_param integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    input_cust_seq varchar;
    input_cust_nm varchar; 
    input_cust_phone varchar; 
    input_cust_email varchar; 
    input_channel_tp varchar;
    input_dml_tp varchar;
    input_uuid varchar;
    error_tf bool;
    error_cnt int;
    asis_uuid varchar;
    tobe_uuid varchar;
    asis_seq int;
    tobe_seq int;
   	asis_register_dt timestamp;
   	tobe_register_dt timestamp;
   	tobe_modify_dt timestamp;
   
   	insert_logic_value_jsonb jsonb;
   	related_uuids varchar[];
   	unify_uuid varchar;
   	len int;
   	d_query text;
   	
   
   -- 예외 처리 변수
    text1 text;
   	text2 text;
   	text3 text;
   
BEGIN

	SELECT  tm.cust_seq,tm.dml_tp,tm.error_tf,tm.error_retry_cnt, lm.channel_tp, um.cust_nm, um.cust_email, um.cust_phone
    INTO    input_cust_seq, input_dml_tp, error_tf, error_cnt, input_channel_tp, input_cust_nm, input_cust_email, input_cust_phone
	FROM    gprtm.task_manager tm
	JOIN	gprtm.legacy_manager lm 
        ON 		tm.lv0_schema_nm = lm.lv0_schema_nm 
        AND 	tm.lv0_table_nm  = lm.lv0_table_nm
    LEFT JOIN    gprtm.union_customer um
        on      tm.cust_seq = um.cust_seq
        and     lm.channel_tp = um.channel_tp    
	WHERE	tm.seq = seq_param;
	
	

	select gprtm.func_search_union_insert(input_cust_nm,input_cust_phone,input_cust_email) into insert_logic_value_jsonb;
	SELECT array_agg(value::text) INTO related_uuids
    FROM jsonb_array_elements_text(insert_logic_value_jsonb->'related_uuids');
	unify_uuid := insert_logic_value_jsonb->>'uuid';
	
	raise notice '%',array_length(related_uuids,1);
	return false;
END;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.func_union_to_link(seq_param integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    input_cust_seq varchar;
    input_cust_nm varchar; 
    input_cust_phone varchar; 
    input_cust_email varchar; 
    input_channel_tp varchar;
    input_dml_tp varchar;
    input_uuid _varchar;
    error_tf bool;
    error_cnt int;
    asis_uuid varchar;
    tobe_uuid varchar;
    asis_seq int;
    tobe_seq int;
   	asis_register_dt timestamp;
   	tobe_register_dt timestamp;
   	tobe_modify_dt timestamp;
   	first_uuid varchar;
    cur record;
   
   	effected_uuid_array _varchar;
    changed_effected_uuid_array _varchar;
   
   	-- 예외 처리 변수
    text1 text;
   	text2 text;
   	text3 text;
   
BEGIN
    RAISE NOTICE 'UNION -> LINK START';

	SELECT  tm.cust_seq,tm.dml_tp,tm.error_tf, coalesce(tm.error_retry_cnt, 0), lm.channel_tp, um.cust_nm, um.cust_email, um.cust_phone
    INTO    input_cust_seq, input_dml_tp, error_tf, error_cnt, input_channel_tp, input_cust_nm, input_cust_email, input_cust_phone
	FROM    gprtm.task_manager tm
	JOIN	gprtm.legacy_manager lm 
        ON 		tm.lv0_schema_nm = lm.lv0_schema_nm 
        AND 	tm.lv0_table_nm  = lm.lv0_table_nm
    LEFT JOIN    gprtm.union_customer um
        on      tm.cust_seq = um.cust_seq
        and     lm.channel_tp = um.channel_tp    
	WHERE	tm.seq = seq_param;

	select seq, uuid, register_dt into asis_seq, asis_uuid, asis_register_dt from gprtm.link_customer where channel_tp = input_channel_tp and cust_seq = input_cust_seq;
	
	if asis_seq is null and input_dml_tp = 'DELETE' then return true;
	end if;
	
    select gprtm.func_return_uuid(input_channel_tp, input_cust_seq, input_cust_nm, input_cust_email, input_cust_phone) into input_uuid;
	
    first_uuid := input_uuid[1];
   	changed_effected_uuid_array := ARRAY[]::varchar[];
--    IF (input_dml_tp = 'INSERT') then
--        raise notice 'UNION INSERT START';
--        INSERT INTO gprtm.link_customer(uuid, channel_tp, cust_seq) VALUES(first_uuid, input_channel_tp, input_cust_seq);
--        RAISE NOTICE 'UNION INSERT END';
--
--    elseif (input_dml_tp = 'UPDATE') THEN
--        raise notice 'UNION UPDATE START';
--        UPDATE gprtm.link_customer SET uuid=first_uuid WHERE channel_tp=input_channel_tp and cust_seq=input_cust_seq;
--       	if asis_uuid != first_uuid then
--       		-- asis_uuid가 달라졌을 때
--       		select gprtm.func_union_to_link_sub(asis_seq, seq_param, asis_uuid) into changed_effected_uuid_array;
--       	end if;
--        RAISE NOTICE 'UNION UPDATE END';
       
	IF (input_dml_tp = 'INSERT' or input_dml_tp = 'UPDATE') then
    	raise notice 'UNION UPSERT START';
    	INSERT INTO gprtm.link_customer(uuid, channel_tp, cust_seq) VALUES(first_uuid, input_channel_tp, input_cust_seq)
    	ON CONFLICT (channel_tp, cust_seq) do 
    	UPDATE SET uuid=first_uuid;
    	if input_dml_tp = 'UPDATE' then
   			select gprtm.func_union_to_link_sub(asis_seq, seq_param, asis_uuid) into changed_effected_uuid_array;
    	end if;
   		RAISE NOTICE 'UNION UPSERT END';
    
    elseif (input_dml_tp = 'DELETE') THEN
        raise notice 'UNION DELETE START';
       	-- Delete from gprtm.link_customer WHERE uuid IN asis_uuid;
       	-- UPDATE gprtm.link_customer set uuid = tobe_uuid where uuid in 
        DELETE FROM gprtm.link_customer WHERE channel_tp = input_channel_tp AND cust_seq = input_cust_seq;
       	select gprtm.func_union_to_link_sub(asis_seq, seq_param, asis_uuid) into changed_effected_uuid_array;
        RAISE NOTICE 'UNION DELETE END';
       
    ELSE 
        RETURN false;
    END if;
   
	select seq, uuid, register_dt, modify_dt into tobe_seq, tobe_uuid, tobe_register_dt, tobe_modify_dt from gprtm.link_customer where channel_tp = input_channel_tp and cust_seq = input_cust_seq;
--	raise notice '%, %',asis_seq, tobe_seq;
	IF (input_dml_tp = 'INSERT' or input_dml_tp = 'UPDATE') then
		INSERT INTO gprtm.link_customer_log
		(seq, task_dml_tp, task_seq, channel_tp, cust_seq, asis_uuid, tobe_uuid, register_dt, modify_dt)
		VALUES(tobe_seq, input_dml_tp, seq_param, input_channel_tp, input_cust_seq, asis_uuid, tobe_uuid, tobe_register_dt, tobe_modify_dt);
	else
		INSERT INTO gprtm.link_customer_log
		(seq, task_dml_tp, task_seq, channel_tp, cust_seq, asis_uuid, tobe_uuid, register_dt, modify_dt)
		VALUES(asis_seq, input_dml_tp, seq_param, input_channel_tp, input_cust_seq, asis_uuid, tobe_uuid, asis_register_dt, timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP));
	end if;

	
    if array_length(input_uuid, 1) > 1 then
	    FOR i IN 2..array_length(input_uuid, 1)
	    loop
		    effected_uuid_array := effected_uuid_array || ARRAY[input_uuid[i]];
		    FOR cur IN SELECT channel_tp,cust_seq FROM gprtm.link_customer WHERE uuid = input_uuid[i]
		    LOOP
		        update gprtm.link_customer set uuid = first_uuid where channel_tp = cur.channel_tp and cust_seq = cur.cust_seq;
				INSERT INTO gprtm.link_customer_log
				(seq, task_dml_tp, task_seq, channel_tp, cust_seq, asis_uuid, tobe_uuid, register_dt, modify_dt)
				VALUES(tobe_seq, 'CHAIN', seq_param, cur.channel_tp, cur.cust_seq, input_uuid[i], first_uuid, timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP), timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP));
		    END LOOP;
	    END LOOP;
	end if;
	
   
    if error_tf = true then
		UPDATE gprtm.task_manager
	   	SET 
	   		link_tf = true,
	   		error_tf = false,
	   		error_retry_cnt = 0,
	   		effected_uuid = effected_uuid_array,
    		changed_effected_uuid = changed_effected_uuid_array
	    WHERE seq = seq_param;
	else 
		UPDATE gprtm.task_manager
		SET link_tf = true,
			effected_uuid = effected_uuid_array,
    		changed_effected_uuid = changed_effected_uuid_array
		WHERE seq = seq_param;
	END IF;

	RETURN true;

    exception WHEN OTHERS then
   	   	get STACKED diagnostics
	        text1 = RETURNed_SQLSTATE,
	        text2 = MESSAGE_TEXT,
	        text3 = PG_EXCEPTION_CONTEXT;
	   
	    raise notice 'returned_sqlstate = %',text1;
	    raise notice 'message_text = %',text2;
	    raise notice 'pg_exception_context = %',text3;
	   	
	   	return false;
   
END;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.func_union_to_link_sub(asis_seq integer, seq_param integer, uuid_param character varying)
 RETURNS character varying[]
 LANGUAGE plpgsql
AS $function$
declare 
	union_link_record gprtm._temp_union_link_table%ROWTYPE;
	check_cnt int := 0;
	check_tf int;
	uuid_value varchar;
	temp_uid varchar;
	cur record;
	return_array _varchar;
begin
	truncate table gprtm._temp_union_link_table;
	truncate table gprtm._change_union_link_table;
	
	insert into gprtm._temp_union_link_table
	select a.uuid, a.channel_tp , a.cust_seq , b.cust_nm , b.cust_phone , b.cust_email, a.register_dt, 0
	from gprtm.link_customer as a
	join gprtm.union_customer as b
	on(a.channel_tp=b.channel_tp and a.cust_seq=b.cust_seq)
	where 1=1
	and a.uuid = uuid_param;

	insert into gprtm._change_union_link_table
	select uuid, channel_tp, cust_seq, cust_nm, cust_phone, cust_email, register_dt, null
	from gprtm._temp_union_link_table ;

	update gprtm._temp_union_link_table as a
	set cnt = b.cnt
	from 
	(
		select a.channel_tp , b.cust_seq , count(*) as cnt
		from gprtm._temp_union_link_table as a
		join gprtm._temp_union_link_table as b 
		on
		(
			(a.cust_nm = b.cust_nm and a.cust_phone = b.cust_phone)
			or (a.cust_nm = b.cust_nm and a.cust_email = b.cust_email)
			or (a.cust_phone = b.cust_phone and a.cust_email = b.cust_email)
		)
		group by 1,2
	) as b
	where 1=1
	and a.channel_tp = b.channel_tp and a.cust_seq = b.cust_seq ;

	return_array := ARRAY[]::varchar[][];
	for union_link_record in select * from gprtm._temp_union_link_table tult order by cnt desc
	loop
		check_tf := 0;
		
		select count(*) into check_tf 
		from gprtm._change_union_link_table
		where 1=1
		and changed_uuid is null
		and channel_tp = union_link_record.channel_tp
		and cust_seq = union_link_record.cust_seq;
		
		if check_tf >= 1 then
			uuid_value := encode(gen_random_bytes(16), 'hex');
	--			raise notice '%',uuid_value;
			update gprtm._change_union_link_table
			set changed_uuid = uuid_value
			where 1=1
			and changed_uuid is null
			and (
				(cust_nm = union_link_record.cust_nm and cust_phone = union_link_record.cust_phone)
				or (cust_nm = union_link_record.cust_nm and cust_email = union_link_record.cust_email)
				or (cust_phone = union_link_record.cust_phone and cust_email = union_link_record.cust_email)
			);
			check_cnt := check_cnt+1;
			
			return_array := return_array || array[uuid_value];
		end if;
	end loop;
	
	raise notice '%',check_cnt;

	if check_cnt <= 1 then
		-- 한 통합 그대로 유지 또는 없음
		raise notice '그대로 유지';
		
		return_array := ARRAY[]::varchar[][];
		return return_array;
	else 
		-- 한 통합이 쪼개짐
		raise notice '쪼개짐';
	
		update gprtm.link_customer as a
		set uuid = b.changed_uuid,
			modify_dt = timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP)
		from gprtm._change_union_link_table as b
		where 1=1
		and a.channel_tp = b.channel_tp 
		and a.cust_seq = b.cust_seq;

		-- log
		FOR cur IN SELECT channel_tp,cust_seq, uuid, changed_uuid FROM gprtm._change_union_link_table
	    LOOP
			INSERT INTO gprtm.link_customer_log
			(seq, task_dml_tp, task_seq, channel_tp, cust_seq, asis_uuid, tobe_uuid, register_dt, modify_dt)
			VALUES(asis_seq, 'CHAIN', seq_param, cur.channel_tp, cur.cust_seq, cur.uuid, cur.changed_uuid, timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP), timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP));
	    END LOOP;
	
		return return_array; 
	end if;
end;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.func_update_modifiy_dt()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.modify_dt := timezone('Asia/Seoul'::text, CURRENT_TIMESTAMP);
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.hc_test(sfid_param text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
declare 

begin 
	if (sfid_param in (select sfid from hc.contact) ) is true then
		--uuid, 전화번호 중복 검사 
		-- 중복키값 변경 함수 실행 성공
			-- sfid 기준으로 update
		raise notice 'success';
		
	end if;
end;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.test_func_rule_manager()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
origin_source jsonb;
cn_tp jsonb;
modified_source jsonb;

origin_source_length int;
modified_source_length int;
counter int;

update_source jsonb;

	BEGIN

	select (((default_json ->> 'condition')::jsonb -> 1) ->> 'Source')::jsonb into origin_source from gprtm.rule_manager;	
	raise notice '%', origin_source;

	SELECT jsonb_agg(channel_tp) INTO modified_source
	FROM (
	    SELECT DISTINCT channel_tp
	    FROM gprtm.legacy_manager
	) AS subquery;

	cn_tp := ('{"Source": ' || modified_source::text || '}')::jsonb;
	raise notice '%', cn_tp;
	
	select jsonb_array_length(modified_source) - 1 into modified_source_length;
	
	FOR counter IN 0..modified_source_length loop
        if not EXISTS (SELECT 1 FROM jsonb_array_elements(origin_source) AS element WHERE element = modified_source->counter) then
			origin_source := origin_source || jsonb_agg(modified_source -> counter);
        end if;
    END LOOP;
   
   	update_source := '[]'::jsonb;
	select jsonb_array_length(origin_source) - 1 into origin_source_length;

	for counter in 0..origin_source_length loop
		if EXISTS (SELECT 1 FROM jsonb_array_elements(modified_source) AS element WHERE element = origin_source->counter) then
			update_source := update_source || jsonb_agg(origin_source -> counter);
        end if;
	end loop;
	
	raise notice '%', update_source;
	
	raise notice '%', origin_source;
   
--	SELECT jsonb_agg(DISTINCT s ORDER BY s) INTO modified_source
--    FROM (
--        SELECT jsonb_array_elements(origin_source->'Source') AS s
--        UNION DISTINCT
--        SELECT jsonb_array_elements(cn_tp->'Source')
--    ) AS subquery;
	
	UPDATE gprtm.rule_manager
	SET default_json = jsonb_set(
    	(default_json::jsonb),
    	'{condition, 1, Source}',
    	update_source
	)::json;
	

	return true;
	END;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.test_sub(asis_seq integer, seq_param integer, uuid_param character varying)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
declare 
	union_link_record gprtm._temp_union_link_table%ROWTYPE;
	check_cnt int := 0;
	check_tf int;
	uuid_value varchar;
	temp_uid varchar;
	cur record;
	return_array _varchar;
begin

	return_array := ARRAY[]::varchar[][];
	for union_link_record in select * from gprtm._temp_union_link_table tult order by cnt desc
	loop
		check_tf := 0;
		
		select count(*) into check_tf 
		from gprtm._change_union_link_table
		where 1=1
		and changed_uuid is null
		and channel_tp = union_link_record.channel_tp
		and cust_seq = union_link_record.cust_seq;
		
		if check_tf >= 1 then
			uuid_value := encode(gen_random_bytes(16), 'hex');
	--			raise notice '%',uuid_value;
			update gprtm._change_union_link_table
			set changed_uuid = uuid_value
			where 1=1
			and changed_uuid is null
			and (
				(cust_nm = union_link_record.cust_nm and cust_phone = union_link_record.cust_phone)
				or (cust_nm = union_link_record.cust_nm and cust_email = union_link_record.cust_email)
				or (cust_phone = union_link_record.cust_phone and cust_email = union_link_record.cust_email)
			);
			check_cnt := check_cnt+1;
			
		end if;
	end loop;
	
	raise notice '%',check_cnt;

	if check_cnt <= 1 then
		-- 한 통합 그대로 유지 또는 없음
		raise notice '그대로 유지';
		
	else 
		-- 한 통합이 쪼개짐
		raise notice '쪼개짐';

		-- log
		FOR cur IN SELECT channel_tp,cust_seq, uuid, changed_uuid FROM gprtm._change_union_link_table
	    LOOP
			raise notice '%, %, %,%',cur.channel_tp, cur.cust_seq, cur.uuid, cur.changed_uuid;	
		END LOOP;
	end if;
end;
$function$
;

CREATE OR REPLACE FUNCTION gprtm.test_trigger_function()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
declare 
	rec record;
begin 
	
	for rec in select seq
				from gprtm.test
				where 1=1
				and seq != new.seq
				and replace(phone,'_','') = new.phone
				order by length(phone) desc
	loop
		update gprtm.test
		set phone = phone || '_'
		where 1=1
		and seq = rec.seq;
		
	end loop;
	

--	update gprtm.test 
--	set phone = phone || '_'
--	where 1=1
--	and seq != new.seq
--	and phone like new.phone || '%';

	return new;
	
end;
$function$
;