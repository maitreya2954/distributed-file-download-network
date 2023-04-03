from flask import request, url_for
from flask_api import FlaskAPI, status, exceptions
from multiprocessing.pool import ThreadPool
from dfdn_admin import initiateDownload

POOL = None
NO_OF_THREADS = 5

POOL = ThreadPool(processes=NO_OF_THREADS)
print('Thread pool created with ' + str(NO_OF_THREADS) + ' threads')

app = FlaskAPI(__name__)

@app.route('/')
def landing():
    return 'Admin flask application'

@app.route('/healthCheck', methods=['GET'])
def serverup():
    return ''

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
