from flask import request, send_from_directory
from flask_api import FlaskAPI, status
from dfdn_helper import downloadChunk, postRequest
from multiprocessing.pool import ThreadPool
from multiprocessing import Manager
import os
import signal
import sys
import time

REMOTE_SERVER='66.71.62.77'
PORT=9999
POOL = None
NO_OF_THREADS = 5

try:
    os.mkdir('chunks')
except Exception as e:
    print(e)
    

POOL = ThreadPool(processes=NO_OF_THREADS)
print('Thread pool created with ' + str(NO_OF_THREADS) + ' threads')

MANAGER = Manager()
print('Created a pool manager')

PROGRESS_DICT = MANAGER.dict()

app = FlaskAPI(__name__)

@app.route('/')
def landing():
    return 'Helper flask application'

@app.route('/v1/healthCheck', methods=['GET'])
def serverup():
    return ''

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
    # print('Progress API', getProgress())
    return {'progress': PROGRESS_DICT[chunkId] if chunkId in PROGRESS_DICT else 0}

@app.route('/v1/chunk/<chunkId>', methods=['GET'])
def chunk(chunkId):
    try:
        return send_from_directory('chunks', chunkId, mimetype='application/octet-stream')
    except Exception as e:
        print('Error while sending the chunk : ', e)
        return 'Error occured', status.HTTP_500_INTERNAL_SERVER_ERROR

def shutdown_hook(signum=None, frame=None):
    print('Shutdown hook invoked. Shutting down.')
    POOL.close()
    PROGRESS_DICT.clear()
    MANAGER.shutdown()
    sys.exit(0)
    
signal.signal(signal.SIGINT, shutdown_hook)

postRequest(REMOTE_SERVER, PORT, 'v1/register', None)

if __name__=='__main__':
    app.run(debug=True)
