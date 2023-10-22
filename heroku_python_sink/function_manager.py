def level_function(session,seq,level,schema_nm,table_nm):
    try:
        query = f'select fm.function_nm from gprtm.function_manager fm where fm.enable_tf = true and fm.level_num = {level} and fm.lv0_schema_nm = \'{schema_nm}\' and fm.lv0_table_nm = \'{table_nm}\' order by fm.priority_num asc'
        result = session.execute(query)
        for i in result.all():
            query = f'select {i[0]}({seq})'
            result = session.execute(query)
            tf = result.all()
            if tf[0][0]:
                print(f'{query} 성공')
                pass
            else:
                print(f'{query} 실패')
                return False
        return True
    
    except Exception as e:
        print(f'task_record {seq} 통합 과정 에러 발생 {e}')
        session.commit()
        return False