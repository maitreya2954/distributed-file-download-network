from flask import request, render_template
from flask_api import FlaskAPI, status, exceptions
from multiprocessing.pool import ThreadPool
from dfdn_admin import initiateDownload, postRequest, monitorProgress, downloadChunk, getProgress
import time
import os

POOL = None
NO_OF_THREADS = 5

POOL = ThreadPool(processes=NO_OF_THREADS)
print('Thread pool created with ' + str(NO_OF_THREADS) + ' threads')

try:
    os.mkdir('chunks')
except Exception as e:
    print(e)
    

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
        # TODO : Take request id from the response
        requestId = str(time.time() * 1000)
        POOL.apply_async(initiateDownload, args=[requestId], callback=monitorProgress)
        return 'Completed'
    except Exception as e:
        print('Error occured while initiating download', e)
        return 'Error occured', status.HTTP_500_INTERNAL_SERVER_ERROR
    
@app.route('/v1/downloadLink', methods=['POST'])
def downloadLink():
    try:
        link = request.form['dlink']
        jsondata = {'link': link}
        # postRequest(dfdn_admin.REMOTE_SERVER, dfdn_admin.PORT, 'v1/begin', data=jsondata)
        return render_template('success.html')
    except Exception as e:
        print('Error occured while sending the link to server', e)
        return render_template('error.html'), status.HTTP_500_INTERNAL_SERVER_ERROR
    
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
