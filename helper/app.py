from flask import request
from flask_api import FlaskAPI, status
from dfdn_helper import downloadChunk, getProgress
from multiprocessing.pool import ThreadPool
from multiprocessing import Manager
import os
import signal
import sys

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
    return {'progress': PROGRESS_DICT[chunkId]}

def shutdown_hook(signum=None, frame=None):
    print('Shutdown hook invoked. Shutting down.')
    POOL.close()
    PROGRESS_DICT.clear()
    MANAGER.shutdown()
    sys.exit(0)
    
signal.signal(signal.SIGINT, shutdown_hook)

if __name__=='__main__':
    app.run(debug=True)
