import json
import requests
import time

REMOTE_SERVER='66.71.23.120'
PORT=9999
    
def downloadHandler(url,reqId,targetIp):
    filename = url.split('/')[-1]
    try:
        r = requests.get(url, allow_redirects=True)
        os.mkdir('./'+reqId)
        open('./'+reqId+'/'+filename, 'wb').write(r.content)
        postRequest(targetIp,PORT, 'v1/ready', jsonify({'requestId':reqId}))    
    except Exception as e:
        raise Exception('Error while downloading file', e)
    
