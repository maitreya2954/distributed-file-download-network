import os
from flask import Flask,request,jsonify
import requests
import threading 
import sqlite3
import time
import click
from flask import current_app, g
from filesplit.split import Split

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # a simple page that says hello
    def downloadHandler(url,reqId):
        filename = url.split('/')[-1]
        print('Downloading :'+filename)
        r = requests.get(url, allow_redirects=True)
        open('./'+reqId+filename, 'wb').write(r.content)

    @app.route('/v1/begin', methods=['POST'])
    def begin():
        url = ''
        req = request.get_json(force=True)
        if 'url' in req:
            url = req['url']
        ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
        #print(ip_addr)
        reqId = str(ip_addr+time.time())
        t1 = threading.Thread(target=downloadHandler, args=(url,reqId,))
        t1.start()
        return jsonify({'requestId':reqId})
    
    @app.route('/v1/helperData', methods=['POST'])
    def helperData():
        req = request.get_json(force=True)
        reqId = req['requestId']
        count = 0
        if 'partitions' in req:
            count = len(req['partitions'])
        filename = ''
        dirName = './'+reqId
        files = od.listdir(dirName)
        for i in files:
            if os.path.isfile(dirName+i):
                filename = i
        fileSize = os.path.getsize(dirName+filename)
        split = Split(dirName+filename,dirName+'/chunks')
        split.bysize(Math.ceil(fileSize/count))
        resMap = {}
        resMap['requestId'] = reqId
        resMap['partitionData'] = []
        for path in os.scandir(dirName+'/chunks'):
            if path.is_file(path):
                resMap['partitionData'].append({'fileName':path.name,'fileSize':path.size})
        return jsonify(resMap)
    
    from . import db
    db.init_app(app)
    
    return app