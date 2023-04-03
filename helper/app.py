from flask import request
from flask_api import FlaskAPI, status

detectedHelpers = ['']
data = None
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
        if 'data' in request.args:
            data = request.args['data']
            print(data)
    except:
        print('Error while processing partition data')
        return 'Error occured', status.HTTP_500_INTERNAL_SERVER_ERROR

if __name__=='__main__':
    app.run(debug=True)
