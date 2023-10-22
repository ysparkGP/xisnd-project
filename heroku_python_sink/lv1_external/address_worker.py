import requests
import json

#API 정보 : 주소기반산업지원서비스
def get_address(address_api_key, keyword):
    try:
        URL = 'https://business.juso.go.kr/addrlink/addrLinkApi.do'
        #검색한 첫번째 결과만 JSON 형태로 가져오도록 설정
        data = {'confmKey': address_api_key, 'currentPage': '1', 'countPerPage':'1','keyword':keyword,'resultType':'json'}
        res = requests.post(URL, data=data)
        return res
    except:
        return None

def run_address_func(session, address_api_key, seq):    
    query = f'select tm.cust_seq,tm.dml_tp,lm.lv1_schema_nm,lm.lv1_table_nm from gprtm.task_manager tm join gprtm.legacy_manager lm on tm.lv0_schema_nm = lm.lv0_schema_nm and tm.lv0_table_nm = lm.lv0_table_nm where tm.seq = \'{seq}\''
    result = session.execute(query).first()
    # print(result)
    lv1_seq = result[0]
    lv1_schema = result[2]
    lv1_table = result[3]

    # 필드 존재 여부 확인
    query = f'SELECT column_name FROM information_schema.columns WHERE table_schema = \'{lv1_schema}\' AND table_name = \'{lv1_table}\' AND column_name = \'cust_addr\';'
    result = session.execute(query)
    # 필드가 존재할 경우 주소 정제 실행
    if result.fetchone(): 
        #테이블의 원본주소 저장된 컬럼명의 데이터 추출
        query = f'select cust_addr from "{lv1_schema}"."{lv1_table}" where cust_seq = \'{lv1_seq}\' and cust_addr is not null'
        result = session.execute(query)
        for i in result.all():
            #네덩이 -> 세덩이 순으로 순차 검증
            chk = False
            for cnt in [4, 3]:
                #원본주소를 뛰어쓰기 기준으로 잘라서 사용 (풀주소 검색은 검색이 안되는 경우가 많음)
                orgaddr = i[0].split(' ')[:cnt]
                #나눠진 덩어리를 다시 붙여서 검색에 사용
                orgaddr = " ".join(orgaddr)
                if len(orgaddr) > 1 and chk == False:
                    res = get_address(address_api_key, orgaddr)
                    #REQUEST 결과 헤더가 200이고, results가 1개 이상일 경우
                    if res.status_code == 200 and int(json.loads(res.text)['results']['common']['totalCount']) > 0:
                        #데이터를 추출하여 우편번호, 시, 도, 군을 추출
                        data = json.loads(res.text)['results']['juso'][0]
                        result_state = data['siNm']
                        result_city = data['sggNm']
                        #DB에 저장
                        query = f'update "{lv1_schema}"."{lv1_table}" set cust_addr_state=\'{result_state}\',cust_addr_city=\'{result_city}\' where cust_seq=\'{lv1_seq}\''
                        session.execute(query)
                        chk = True
                        break
    return True