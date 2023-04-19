from flask import request, render_template
from flask_api import FlaskAPI, status, exceptions
from multiprocessing.pool import ThreadPool
from multiprocessing import Manager
from dfdn_admin import initiateDownload, postRequest, getRequest, downloadChunk, REMOTE_SERVER, PORT
import os
import sys
import signal
import traceback
from requests.exceptions import ReadTimeout
import time

POOL = None
NO_OF_THREADS = 5

POOL = ThreadPool(processes=NO_OF_THREADS)
print('Thread pool created with ' + str(NO_OF_THREADS) + ' threads')

MANAGER = Manager()
print('Created a pool manager')

PROGRESS_DICT = MANAGER.dict()
PARTITIONS_DICT = MANAGER.dict()

try:
    os.mkdir('chunks')
except Exception as e:
    print(e)
    
import socket
hostname = socket.gethostname()
ip_address = socket.gethostbyname(hostname)
print(f"Hostname: {hostname}")
print(f"IP Address: {ip_address}")
    

app = FlaskAPI(__name__)

@app.route('/')
@app.route('/index')
def landing():
    return render_template('index.html', title='Admin App', name='Siddharth')

@app.route('/healthCheck', methods=['GET'])
def serverup():
    return ''

@app.route('/v1/ready', methods=['POST'])
def ready():
    try:
        requestId = request.json['requestId']
        # TEST MARKER
        # requestId = str(time.time())
        POOL.apply_async(initiateDownload, args=[requestId, PARTITIONS_DICT])
        return 'Completed'
    except Exception as e:
        print('Error occured while initiating download')
        traceback.print_exc()
        return 'Error occured', status.HTTP_500_INTERNAL_SERVER_ERROR
    
@app.route('/v1/downloadLink', methods=['POST'])
def downloadLink():
    try:
        link = request.form['dlink']
        jsondata = {'url': link}
        res = postRequest(REMOTE_SERVER, PORT, 'v1/begin', data=jsondata)
        requestId = res.json['requestId']
        requestId = str(time.time())
        return render_template('success.html', requestId=requestId)
    except Exception as e:
        print('Error occured while sending the link to server', e)
        return render_template('error.html'), status.HTTP_500_INTERNAL_SERVER_ERROR
    
@app.route('/v1/partitionData', methods=['POST'])
def partitiondata():
    try:
        partition = request.json
        print(partition)
        POOL.apply_async(downloadChunk, args=[partition, PROGRESS_DICT])
        return ''
    except Exception as e:
        print('Error while processing partition data : ', e)
        return 'Error occured', status.HTTP_500_INTERNAL_SERVER_ERROR

@app.route('/v1/progress/<chunkId>', methods=['GET'])
def progress(chunkId):
    return {'progress': PROGRESS_DICT[chunkId] if chunkId in PROGRESS_DICT else 0}

@app.route('/v1/partitions/<requestId>', methods=['GET'])
def partitions(requestId):
    return PARTITIONS_DICT[requestId]

def shutdown_hook(signum=None, frame=None):
    print('Shutdown hook invoked. Shutting down.')
    POOL.close()
    PROGRESS_DICT.clear()
    MANAGER.shutdown()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_hook)
    
@app.route('/v1/test', methods=['POST'])
def testApi():
    try:
        getRequest('127.0.0.1', 9998, 'v1/healthCheck')
        return ''
    except ReadTimeout as e:
        print('read time exception occured')
        return 'timeout', status.HTTP_500_INTERNAL_SERVER_ERROR
    except:
        traceback.print_exc()
        return 'Error occured', status.HTTP_500_INTERNAL_SERVER_ERROR

print(app.config["SERVER_NAME"])
registerData = {'addr': '66.71.23.120', 'port': '9999'}
postRequest(REMOTE_SERVER, PORT, 'v1/register', registerData)

if __name__=='__main__':
    app.run(debug=True)
