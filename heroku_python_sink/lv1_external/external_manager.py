from .address_worker import run_address_func

def lv1_external(session,address_api_key,seq):
    address_tf = True
    dml_tp = False
    
    query = f'select tm.dml_tp, coalesce(tm.error_retry_cnt, 0) from gprtm.task_manager tm where tm.seq = \'{seq}\''
    result = session.execute(query)
    for i in result.all():
        dml_tp = i[0]
        error_cnt = i[1]

    if dml_tp != 'DELETE':
        # 실행할 코드 아래에서 함수 형태로 호출
        address_tf = run_address_func(session,address_api_key,seq)
        
    if address_tf:
        # 모두 완료 시 lv1_external_tf true로 변경 error_tf를 false로 바꾸고 error_retry_cnt를 0으로 초기화
        query = f'update gprtm.task_manager set lv1_external_tf = true where seq = \'{seq}\''
        update_error_query = f'update gprtm.task_manager set lv1_external_tf = true, error_tf = false, error_retry_cnt = 0 where seq = \'{seq}\''
        session.execute(update_error_query if error_cnt else query)
        return True
    else:
        # 오류 발생 시 error_tf true로 변경하고 error_retry_cnt를 1 증가시킴
        # query = f'update gprtm.task_manager set error_tf = true, error_retry_cnt = \'{error_cnt + 1}\' where seq = \'{seq}\''
        # session.execute(query)
        return False