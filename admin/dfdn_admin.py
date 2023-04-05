import sys
import requests
import time

REMOTE_SERVER='127.0.0.1'
PORT=9999
PROGRESS=0

def downloadComplete(partitionInfo):
    done = 1
    for partition in partitionInfo:
        done = done & (1 if partition['progress'] == 100 else 0)
    return done

def monitorProgress(partitionInfo):
    try:
        print('Progress monitor started')
        for partition in partitionInfo:
            partition['progress'] = 0
        
        while not downloadComplete(partitionInfo):  
            printstring = '' 
            for partition in partitionInfo:
                chunkId = partition['reqId'] + '-' + str(partition['chunk'])
                res = getRequest(partition['addr'], partition['port'], '/v1/progress/' + chunkId)
                partition['progress'] = res.json()['progress']
                printstring += partition['addr'] + ':' + str(partition['port']) + ' - ' + str(partition['progress']) + ' | '
            sys.stdout.write("\r" + printstring)
            sys.stdout.flush()
            time.sleep(1)
        print('\n>>> Download Completed')
    except Exception as e:
        print('Error while monitoring progress', e)
    
def initiateDownload(requestId):
    try:
        time.sleep(1)
        partitionInfo = _SendHelperData()
        _SendPartitionData(partitionInfo, requestId)
        return partitionInfo['partitions']
    except Exception as e:
        print('Exception while initiating download', e)
    
def _SendPartitionData(partitionData, requestId):
    if 'partitions' in partitionData:
        for partition in partitionData['partitions']:
            # if partition['port'] == PORT:
            #     continue
            partition['src'] = _BuildUrl(REMOTE_SERVER, PORT, 'v1/chunk')
            partition['reqId'] = requestId
            # print('Sending partition info to', partition['addr'], partition['port'], partition)
            postRequest(partition['addr'], partition['port'], 'v1/partitionData', partition)
    else:
        raise Exception('Partitions not found in the partition data')
    
def postRequest(addr, port, path, data, protocol='http'):
    try:
        res = requests.post(_BuildUrl(addr, port, path, protocol), json=data)
        if res.status_code != 200:
            raise Exception('Request failed. Status code ' + str(res.status_code) + ' received - ' + res.reason)
    except Exception as e:
        raise e
    return res

def getRequest(addr, port, path, params=None, protocol='http'):
    try:
        res = requests.get(url=_BuildUrl(addr, port, path, protocol), params=params)
        if res.status_code != 200:
            raise Exception('Request failed. Status code ' + str(res.status_code) + ' received - ' + res.reason)
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
    res = {'partitions': [
        {'addr': '127.0.0.1',
        'port': 9999,
        'chunk': 0},
        {'addr': '127.0.0.1',
        'port': 9998,
        'chunk': 1},
        {'addr': '127.0.0.1',
        'port': 9997,
        'chunk': 2}]}
    return res

def _BuildUrl(addr, port, path, protocol='http'):
    return protocol + '://' + addr + ':' + str(port) + ('/' if not path.startswith('/') else '') + path

def _FindHelpers():
    detectedHelpers = [{'addr': '127.0.0.1',
                        'port': 9999},
                       {'addr': '127.0.0.1',
                        'port': 9998},
                       {'addr': '127.0.0.1',
                        'port': 9997}]
    return detectedHelpers

def downloadChunk(partition):
    try:
        # https://speed.hetzner.de/100MB.bin
        print('Starting download from', partition['src'], 'with chunk', partition['chunk'])
        file_name = 'chunks/' + partition['reqId'] + '-' + str(partition['chunk'])
        # link = partition['src']
        link = 'https://speed.hetzner.de/100MB.bin'
        with open(file_name, "wb") as f:
            print("Downloading %s" % file_name)
            response = requests.get(link, stream=True)
            total_length = response.headers.get('content-length')

            if total_length is None: # no content length header
                f.write(response.content)
            else:
                dl = 0
                total_length = int(total_length)
                for data in response.iter_content(chunk_size=4096):
                    dl += len(data)
                    f.write(data)
                    PROGRESS = int((dl*100)/total_length)
                    # sys.stdout.write('\rProgress: ' + str(PROGRESS))    
                    # sys.stdout.flush()
                    # done = int(50 * dl / total_length)
                    # sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50-done)) )    
                    # sys.stdout.flush()
    except Exception as e:
        print('Error occured while downloading the chunk', e)

        
def getProgress():
    return PROGRESS