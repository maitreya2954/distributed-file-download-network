import json
import requests
import time

REMOTE_SERVER='66.71.23.120'
PORT=9999
    
def initiateDownload():
    try:
        time.sleep(3)
        partitionInfo = _SendHelperData()
        _SendPartitionData(partitionInfo)
    except Exception as e:
        print('Exception while initiating download', e)
    
def _SendPartitionData(partitionData):
    if 'partitions' in partitionData:
        for partition in partitionData['partitions']:
            partition['remote'] = _BuildUrl(REMOTE_SERVER, PORT, 'v1/chunk')
            # print('Sending partition info to', partition['addr'], partition['port'], partition)
            _PostRequest(partition['addr'], partition['port'], 'v1/partitionData', partition)
    else:
        raise Exception('Partitions not found in the partition data')
    
def _PostRequest(addr, port, path, data, protocol='http'):
    try:
        res = requests.post(_BuildUrl(addr, port, path, protocol), json=data)
    except Exception as e:
        raise e
    return res

def _SendHelperData():
    try:
        helpers = _FindHelpers()
        # _PostRequest(REMOTE_SERVER, PORT, 'v1/helpersdata', helpers)
    except Exception as e:
        raise Exception('Error while finding helper node', e)
        # TODO send abort to server
    res = {'partitions': [{
        'addr': '66.71.23.120',
        'port': 9998,
        'part': 0},
        {'addr': '66.71.23.120',
        'port': 9997,
        'part': 1}]}
    return res

def _BuildUrl(addr, port, path, protocol='http'):
    return protocol + '://' + addr + ':' + str(port) + ('/' if not path.startswith('/') else '') + path

def _FindHelpers():
    detectedHelpers = [{'addr': '66.71.23.120',
                        'port': 9998},
                       {'addr': '66.71.23.120',
                        'port': 9997}]
    return detectedHelpers