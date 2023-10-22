import os
import json
import requests

heroku_url = os.environ['HEROKU_APP_DEFAULT_DOMAIN_NAME']
DI_connector = os.environ['DI_CONNECTOR']
OM_connector = os.environ['OM_CONNECTOR']
url = f'https://{heroku_url}/connectors'
connector_info = {'sink-DI-postgres':DI_connector,'sink-OM-postgres':OM_connector}

# 커넥터 조회
try:
    response = requests.get(url)
    if response.status_code == 200:
        connectors = response.json()
        run_list = [i for i in connector_info.keys() if i not in connectors]
    else:
        print(f"Error: {response.status_code} - {response.text}")

except Exception as e:
    print(f"An error occurred: {e}")
    run_list = []

# 커넥터 실행
for i in run_list:
    data_dict = json.loads(connector_info[i])
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, json=data_dict, headers=headers)
    print(response.status_code)
    print(response.json())

