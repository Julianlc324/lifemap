import os
import requests

body = requests.get('http://localhost:5000/health', timeout=30).json()
assert body['status'] == 'ok', body
assert body['services']['kafka']['consumer_status'] == 'ok', body['services']['kafka']
assert body['commit'] == os.environ['GIT_COMMIT'], body['commit']
print(body['commit'])
