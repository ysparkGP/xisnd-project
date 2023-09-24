def level_function(session,seq,level,schema_nm,table_nm):
    try:
        error_cnt_query = f'select coalesce(tm.error_retry_cnt, 0) from gprtm.task_manager tm where tm.seq = \'{seq}\''
        error_cnt = session.execute(error_cnt_query).all()[0][0]
        query = f'select fm.function_nm from gprtm.function_manager fm where fm.enable_tf = true and fm.level_num = {level} and fm.lv0_schema_nm = \'{schema_nm}\' and fm.lv0_table_nm = \'{table_nm}\' order by fm.priority_num asc'
        result = session.execute(query)
        for i in result.all():
            query = f'select {i[0]}({seq})'
            # print(query)
            result = session.execute(query)
            if result.all()[0][0]:
                pass
            else:
                # 오류 발생 시 error_tf true로 변경
                query = f'update gprtm.task_manager set error_tf = true, error_retry_cnt = \'{error_cnt + 1}\' where seq = \'{seq}\''
                session.execute(query)
                return False

        if level == 2:
            # 모두 완료 시 lv2_tf true로 변경
            # 만약 재시도 중 성공했다면 error를 false로 바꾸고 error_retry_cnt를 0으로 변경
            query = f'update gprtm.task_manager set lv{level}_tf = true where seq = \'{seq}\''
            update_error_query = f'update gprtm.task_manager set lv{level}_tf = true, error_tf = false, error_retry_cnt = 0 where seq = \'{seq}\''
            session.execute(update_error_query if error_cnt else query)
        return True
    
    except Exception as e:
        return False