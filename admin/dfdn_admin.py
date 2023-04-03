import json
import requests
import time

REMOTE_SERVER='66.71.23.120'
PORT=9999
    
def initiateDownload():
    time.sleep(3)
    partitionInfo = _SendHelperData()
    _SendPartitionData(partitionInfo)
    
def _SendPartitionData(partitionData):
    if 'partitions' in partitionData:
        for partition in partitionData['partitions']:
            _PostRequest(partition['addr'], partition['port'], 'v1/partitiondata', partition)
    else:
        raise Exception('Partitions not found in the partition data')
    
def _PostRequest(addr, port, path, data, protocol='http'):
    return requests.post(_BuildUrl(addr, port, path, protocol), json=data)

def _SendHelperData():
    try:
        helpers = _FindHelpers()
        # _PostRequest(REMOTE_SERVER, PORT, 'v1/helpersdata', helpers)
    except Exception as e:
        print('Error while finding helper nodes: ', e)
        # TODO send abort to server
    res = {'partitions': [{
        'addr': '66.71.23.120',
        'port': 9998,
        'part': 0},
        {'addr': '66.71.23.120',
        'port': 9997,
        'part': 1}]}
    return res

def _BuildUrl(addr, port, path, protocol):
    return protocol + '://' + addr + ':' + str(port) + '/' if path[0] == '/' else '' + path

def _FindHelpers():
    detectedHelpers = [{'addr': '66.71.23.120',
                        'port': 9998},
                       {'addr': '66.71.23.120',
                        'port': 9997}]
    return detectedHelpers