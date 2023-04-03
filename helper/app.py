from flask import request
from flask_api import FlaskAPI, status
from dfdn_helper import startDownload
from multiprocessing.pool import ThreadPool

POOL = None
NO_OF_THREADS = 5

POOL = ThreadPool(processes=NO_OF_THREADS)
print('Thread pool created with ' + str(NO_OF_THREADS) + ' threads')

app = FlaskAPI(__name__)

@app.route('/')
def landing():
    return 'Helper flask application'

@app.route('/v1/healthcheck', methods=['GET'])
def serverup():
    return ''

@app.route('/v1/partitiondata', methods=['POST'])
def partitiondata():
    try:
        partition = request.json
        print(partition)
        POOL.apply_async(startDownload, args=[partition])
        return ''
    except Exception as e:
        print('Error while processing partition data : ', e)
        return 'Error occured', status.HTTP_500_INTERNAL_SERVER_ERROR

if __name__=='__main__':
    app.run(debug=True)
