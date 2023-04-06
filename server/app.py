from flask import request, url_for
from flask_api import FlaskAPI, status, exceptions
from multiprocessing.pool import ThreadPool
from filesplit.split import Split
import os
import Math
import json
#from dfdn_server import downloadHandler

POOL = None
NO_OF_THREADS = 5

POOL = ThreadPool(processes=NO_OF_THREADS)
print('Thread pool created with ' + str(NO_OF_THREADS) + ' threads')

app = FlaskAPI(__name__)

def downloadHandler(url,reqId,targetIp):
    filename = url.split('/')[-1]
    try:
        r = requests.get(url, allow_redirects=True)
        os.mkdir('./'+reqId)
        open('./'+reqId+'/'+filename, 'wb').write(r.content)
        postRequest(targetIp,PORT, 'v1/ready', json.dumps({'requestId':reqId},0))    
    except Exception as e:
        raise Exception('Error while downloading file', e)

@app.route('/')
def landing():
    return 'Server flask application'

@app.route('/healthCheck', methods=['GET'])
def serverup():
    return ''

@app.route('/v1/begin', methods=['POST'])
def begin():
    try:
        url = ''
        req = request.get_json(force=True)
        if 'url' in req:
            url = req['url']
        ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
        reqId = str(ip_addr+str(time.time()))
        POOL.apply_async(downloadHandler, args=[url,reqId,ip_addr])
        return '200'
    except Exception as e:
        print('Error occured while initiating download', e)
        return 'Error occured', status.HTTP_500_INTERNAL_SERVER_ERROR

@app.route('/v1/helperData', methods=['POST'])
def helperData():
    try:
        req = request.get_json(force=True)
        reqId = req['requestId']
        count = 0
        if 'partitions' in req:
            count = len(req['partitions'])
        filename = ''
        dirName = './'+reqId
        if not os.path.exists(dirName+'/chunks'):
            os.mkdir(dirName+'/chunks')
        for i in os.listdir(dirName):
            if os.path.isfile(dirName+'/'+i):
                filename = i
        fileSize = os.path.getsize(dirName+'/'+filename)
        split = Split(dirName+'/'+filename,dirName+'/chunks')
        split.bysize(Math.ceil(fileSize/(count-1)))
        resMap = {}
        resMap['requestId'] = reqId
        resMap['partitionData'] = []
        i = 0
        for path in os.scandir(dirName+'/chunks'):
            if path.is_file(path) and path.name != 'manifest':
                resMap['partitionData'].append({'fileName':path.name,'fileSize':os.path.getsize(path),'helper':req['partitions'][i]['addr']})
                i += 1
        return json.dumps(resMap,0)
    except Exception as e:
        print('Error occured while splitting file', e)
        return 'Error occured', status.HTTP_500_INTERNAL_SERVER_ERROR

@app.route('/v1/ready', methods=['POST'])
def ready():
    try:
        if 'data' in request.args:
            data = request.args['data']
        POOL.apply_async(initiateDownload)
        return 'Completed'
    except Exception as e:
        print('Error occured while initiating download', e)
        return 'Error occured', status.HTTP_500_INTERNAL_SERVER_ERROR

if __name__=='__main__':
    app.run(debug=True)
