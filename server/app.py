from flask import request, url_for,jsonify,send_from_directory
from flask_api import FlaskAPI, status, exceptions
from multiprocessing.pool import ThreadPool
from filesplit.split import Split
from flask.templating import render_template
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate, migrate
import os
import math
import time
import requests
import traceback
import hashlib
import shutil
import sys

PORT = '9999'

POOL = None
NO_OF_THREADS = 5

POOL = ThreadPool(processes=NO_OF_THREADS)
print('Thread pool created with ' + str(NO_OF_THREADS) + ' threads')

app = FlaskAPI(__name__)

# adding configuration for using a sqlite database
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///site.db'
 
# Creating an SQLAlchemy instance
db = SQLAlchemy(app)
 
# Settings for migrations
migrate = Migrate(app, db)

# Models
class Node(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ip = db.Column(db.String(20), unique=False, nullable=False)
    port = db.Column(db.String(5), unique=False, nullable=False)
 
    # repr method represents how one object of this datatable
    # will look like
    def __repr__(self):
        return f"ip address : {self.ip}, port: {self.port}"
    
class Request(db.Model):
    id = db.Column(db.String(50), primary_key=True)
    url = db.Column(db.String(200), unique=False, nullable=False)
    fileName = db.Column(db.String(5), unique=False, nullable=False)

    # repr method represents how one object of this datatable
    # will look like
    def __repr__(self):
        return f"RequestId : {self.id}, url: {self.url}, fileName: {self.fileName}"

def downloadHandler(url,reqId,targetIp,foundRecent,R):
    filename = url.split('/')[-1]
    
    try:
        dir = './'+reqId
        os.mkdir(dir)
        if foundRecent and os.path.exists('./'+R.id+'/'+R.fileName):
            shutil.copy2('./'+R.id+'/'+R.fileName,'./'+reqId)
        else:
            print('Starting download for', url)
            with open(dir+'/'+filename, "wb") as f:
                response = requests.get(url, stream=True)
                total_length = response.headers.get('content-length')

                if total_length is None:  # no content length header
                    f.write(response.content)
                else:
                    dl = 0
                    total_length = int(total_length)
                    for data in response.iter_content(chunk_size=8192):
                        dl += len(data)
                        f.write(data)
                        progress = int((dl*100)/total_length)
                        sys.stdout.write('\rProgress: ' + str(progress))
                        sys.stdout.flush()
        
        postRequest(targetIp,PORT, 'v1/ready', {'requestId':reqId})    
    except Exception as e:
        traceback.print_exc()
        print('Error while downloading file', e)

@app.route('/v1/register', methods=["POST"])
def registerNode():
    
    reqIp = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)

    N1 = Node.query.filter_by(ip=reqIp, port = PORT).first()
    #print(N1)
    if N1 is None:
        N = Node(ip = reqIp, port = PORT)
        db.session.add(N)
        db.session.commit()
        #print(N)
    return ''

@app.route('/v1/getNodes', methods=["GET"])
def getNodes():
    Nodes = Node.query.all()
    res = []
    ipAddr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    reqIp = ipAddr.split('.')
    for N in Nodes:
        nodeIp = N.ip.split('.')
        if nodeIp[0] == reqIp[0] and nodeIp[1] == reqIp[1]:
            node = {'addr':N.ip,'port':N.port}
            if ipAddr == N.ip:
                res.insert(0,node)
            else:
                res.append(node)
    return res

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
        #print(req)
        if 'url' in req:
            url = req['url']
        else:
            return 'url not found', status.HTTP_500_INTERNAL_SERVER_ERROR
        filename = url.split('/')[-1]
        ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
        reqId = str(ip_addr+'-'+str(time.time()))

        R = Request.query.filter_by(url=url).first()
        foundRecent = False
        if R is not None:
            foundRecent = True
        else:
            newR = Request(id = reqId,url = url,fileName = filename)
            db.session.add(newR)
            db.session.commit()

        POOL.apply_async(downloadHandler, args=[url,reqId,ip_addr,foundRecent,R])
        resObj = {'requestId':reqId}
        return resObj
    except Exception as e:
        print('Error occured while initiating download', e)
        traceback.print_exc()
        return 'Error occured', status.HTTP_500_INTERNAL_SERVER_ERROR

@app.route('/v1/helpersData', methods=['POST'])
def helperData():
    try:
        req = request.json
        print('creating chunks for request'+str(req['requestId'])+str(req['helpers']))
        reqId = req['requestId']
        count = 0
        if 'helpers' in req:
            count = len(req['helpers'])
        filename = ''
        dirName = './'+reqId
        if not os.path.exists(dirName+'/chunks'):
            os.mkdir(dirName+'/chunks')
        for i in os.listdir(dirName):
            if os.path.isfile(dirName+'/'+i):
                filename = i
        fileSize = os.path.getsize(dirName+'/'+filename)
        split = Split(dirName+'/'+filename,dirName+'/chunks')
        splitSize = math.ceil(fileSize/(count)) + 5*1024*1024
        print('spliiting size'+str(splitSize))
        split.bysize(splitSize)
        resMap = {}
        resMap['requestId'] = reqId
        resMap['partitionData'] = []
        i = 0
        ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
        for path in os.scandir(dirName+'/chunks'):
            if path.is_file() and path.name != 'manifest': 
                if request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr) == req['helpers'][i]['addr']:
                    resMap['partitionData'].insert(0,{'fileName':path.name,'fileSize':os.path.getsize(path),'addr':req['helpers'][i]['addr'],'port':req['helpers'][i]['port']})
                else:    
                    resMap['partitionData'].append({'fileName':path.name,'fileSize':os.path.getsize(path),'addr':req['helpers'][i]['addr'],'port':req['helpers'][i]['port']})
                i += 1
        manifestF = open(dirName+'/chunks/manifest','r')
        manifest = manifestF.read()
        resMap['manifest'] = manifest
        resMap['originalName']= filename
        resMap['checksum'] = hashlib.md5(open(dirName+'/'+filename,'rb').read()).hexdigest()
        return jsonify(resMap)
    except Exception as e:
        traceback.print_exc()
        print('Error occured while splitting file :', e)
        return 'Error occured', status.HTTP_500_INTERNAL_SERVER_ERROR

@app.route('/v1/chunk/<requestId>/<chunkId>', methods=['GET'])
def chunk(requestId,chunkId):
    try:
        return send_from_directory('./'+requestId+'/chunks/',chunkId, mimetype='application/octet-stream')
    except Exception as e:
        print('Error while sending the chunk : ', e)
        traceback.print_exc()
        return 'Error occured', status.HTTP_500_INTERNAL_SERVER_ERROR


def postRequest(addr, port, path, data, protocol='http'):
    try:
        res = requests.post(_BuildUrl(addr, port, path, protocol), json=data)
        if res.status_code != 200:
            raise Exception('Request failed. Status code ' + str(res.status_code) + ' received - ' + res.reason)
    except Exception as e:
        raise e
    return 

def clear_data(session):
    meta = db.metadata
    for table in reversed(meta.sorted_tables):
        print ('Clear table %s' % table)
        session.execute(table.delete())
    session.commit()    

@app.route('/v1/clearData', methods=['GET'])
def clearData():
    db.drop_all()
    db.create_all()
    return ''

def _BuildUrl(addr, port, path, protocol='http'):
    return protocol + '://' + addr + ':' + str(port) + ('/' if not path.startswith('/') else '') + path


if __name__=='__main__':
    
    db.create_all()
    app.run(debug=True)
    
