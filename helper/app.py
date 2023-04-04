from flask import request
from flask_api import FlaskAPI, status
from dfdn_helper import downloadChunk, getProgress
from multiprocessing.pool import ThreadPool
import os

POOL = None
NO_OF_THREADS = 5

try:
    os.mkdir('chunks')
except Exception as e:
    print(e)
    

POOL = ThreadPool(processes=NO_OF_THREADS)
print('Thread pool created with ' + str(NO_OF_THREADS) + ' threads')

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
        POOL.apply_async(downloadChunk, args=[partition])
        return ''
    except Exception as e:
        print('Error while processing partition data : ', e)
        return 'Error occured', status.HTTP_500_INTERNAL_SERVER_ERROR

@app.route('/v1/progress', methods=['GET'])
def progress():
    # print('Progress API', getProgress())
    return {'progress': getProgress()}

if __name__=='__main__':
    app.run(debug=True)
