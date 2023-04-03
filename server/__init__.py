import os
from flask import Flask, request
import requests
import threading 
import sqlite3

import click
from flask import current_app, g


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
    def downloadHandler(url):
        filename = url.split('/')[-1]
        print('Downloading :'+filename)
        r = requests.get(url, allow_redirects=True)
        open(filename, 'wb').write(r.content)

    @app.route('/v1/begin', methods=['POST'])
    def begin():
        url = request.form['url']
        ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
        print(ip_addr)
        t1 = threading.Thread(target=downloadHandler, args=(url,))
        t1.start()
        return "200"

    
    from . import db
    db.init_app(app)
    
    return app