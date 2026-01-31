import requests

requests.packages.urllib3.disable_warnings()

@service
def shutdown():
  url = "https://pvebkp.local.zimbres.com:8006/api2/json/nodes/pvebkp/status"

  payload = 'command=shutdown'
  headers = {
    'Authorization': 'PVEAPIToken=root@pam!token=0ada61e4-deab-4965-9130-582acd032849'
  }

  response = task.executor(requests.post, url, headers=headers, data=payload, verify=False)

  print(response.text)