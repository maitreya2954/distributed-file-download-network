import sys
import requests
import time
import os
from filesplit.merge import Merge
import shutil
import traceback
import hashlib
from requests.exceptions import ReadTimeout,ConnectionError
import random

REMOTE_SERVER='66.71.62.77'
# REMOTE_SERVER='10.0.0.209'
PORT=9999
TIMEOUT=5 # 5 seconds
MAX_RETRIES=3
BACKOFF_FACTOR=1

def downloadComplete(partitionInfo):
    done = 1
    for partition in partitionInfo:
        done = done & (1 if partition['progress'] == 100 else 0)
    return done


def reassignPartition(partition, allpartitions):
    # TEST MARKER
    # modpartitions = [d for d in allpartitions if d['port'] != partition['port']]
    modpartitions = [d for d in allpartitions if d['addr'] != partition['addr']]
    reassignedPartition = random.choice(modpartitions)
    partition['addr'] = reassignedPartition['addr']
    partition['port'] = reassignedPartition['port']
    partition['progress'] = 0
    partition['admin'] = reassignedPartition['admin']
    
    # print('Sending partition info to', partition['addr'], partition['port'], partition)
    postRequest(partition['addr'], partition['port'], 'v1/partitionData', partition)
    

def _MonitorProgress(partitionData):
    try:
        partitionInfo = partitionData['partitionData']
        print('Progress monitor started')
        for partition in partitionInfo:
            partition['progress'] = 0
        
        while not downloadComplete(partitionInfo):  
            printstring = '' 
            for partition in partitionInfo:
                print(partition)
                chunkId = partition['reqId'] + '-' + str(partition['fileName'])
                if partition['progress'] != 100:
                    try:
                        res = getRequest(partition['addr'], partition['port'], '/v1/progress/' + chunkId)
                    except:
                        print('Re-assigning',partition['fileName'],'to different nodes')
                        reassignPartition(partition, partitionInfo)
                    partition['progress'] = res.json()['progress']
                printstring += partition['addr'] + ':' + str(partition['port']) + ' - ' + str(partition['progress']) + ' | '
            sys.stdout.write("\r" + printstring)
            sys.stdout.flush()
            time.sleep(2)
        print('\n>>> Chunks downloaded on all helpers. Starting gathering chunks')
        return partitionInfo
    except Exception as e:
        raise e
        
def _GatherChunks(requestId, partitionInfo):
    try:
        downloadDir = 'downloads/' + requestId + '/chunks'
        os.makedirs(downloadDir)
        for partition in partitionInfo['partitionData']:
            chunkId = partition['reqId'] + '-' + str(partition['fileName'])
            if partition['admin']:
                shutil.move('chunks/'+chunkId, downloadDir + '/' + partition['fileName'])
            else:
                print('Getting chunk: ', chunkId)
                response = getRequest(partition['addr'], partition['port'], '/v1/chunk/' + chunkId)
                with open(downloadDir + '/' + partition['fileName'], 'wb') as f:
                    f.write(response.content)
                print(chunkId, 'received')
        print('Merging files')
        open(downloadDir + '/manifest', "w").write(partitionInfo['manifest'])
        merge = Merge(downloadDir, 'downloads/' + requestId, partitionInfo['originalName'])
        merge.merge(cleanup=True)
        with open('downloads/' + requestId + '/' + partitionInfo['originalName'],"rb") as f:
            bytes = f.read()
            if hashlib.md5(bytes).hexdigest() == partitionInfo['checksum']:
                print('Merge successfully completed')
                os.system('open downloads/' + requestId + '/' + partitionInfo['originalName'])
            else:
                raise Exception('File corrupted')
    except Exception as e:
        raise e
    
def initiateDownload(requestId, partitiondict):
    try:
        partitionData = _SendHelperData(requestId)
        _SendPartitionData(partitionData, requestId)
        _MonitorProgress(partitionData)
        _GatherChunks(requestId, partitionData)
    except Exception as e:
        print('Exception while initiating download', e)
        traceback.print_exc()
    
def _SendPartitionData(partitionData, requestId):
    # print(partitionData)
    if 'partitionData' in partitionData:
        try:
            admin=True
            for partition in partitionData['partitionData']:
                partition['admin'] = admin
                admin = False
                partition['src'] = _BuildUrl(REMOTE_SERVER, PORT, 'v1/chunk/' + requestId + '/' + partition['fileName'])
                partition['reqId'] = requestId
                
                # print('Sending partition info to', partition['addr'], partition['port'], partition)
                postRequest(partition['addr'], partition['port'], 'v1/partitionData', partition)
        except Exception as e:
            raise e
    else:
        raise Exception('Partitions not found in the partition data')
    
def postRequest(addr, port, path, data, protocol='http'):
    try:
        url=_BuildUrl(addr, port, path, protocol)
        print('Post request :', url)
        for num_retries in range(MAX_RETRIES+1):
            if num_retries > 0:
                sleeptime = BACKOFF_FACTOR*(2**(num_retries -1))
                print('Retrying after ',sleeptime, 'seconds')
                time.sleep(sleeptime)
            try:
                res = requests.post(url, json=data, timeout=5)
                if res == None or res.status_code != 200:
                    raise Exception('Request failed. Status code ' + str(res.status_code) + ' received - ' + res.reason, res)
                else:
                    return res
            except ConnectionError as c:
                print('Connection error occured.')
                if num_retries == MAX_RETRIES:
                    raise c
                else:
                    pass
            except ReadTimeout as t:
                print('Timeout occured')
                if num_retries == MAX_RETRIES:
                    raise t
                else:
                    pass
                pass
    except Exception as e:
        raise e

def getRequest(addr, port, path, params=None, protocol='http'):
    try:
        url=_BuildUrl(addr, port, path, protocol)
        print('Get request :', url)
        for num_retries in range(MAX_RETRIES+1):
            if num_retries > 0:
                sleeptime = BACKOFF_FACTOR*(2**(num_retries -1))
                print('Retrying after ',sleeptime, 'seconds')
                time.sleep(sleeptime)
            try:
                res = requests.get(url, params=params, timeout=5)
                if res == None or res.status_code != 200:
                    raise Exception('Request failed. Status code ' + str(res.status_code) + ' received - ' + res.reason, res)
                else:
                    return res
            except ConnectionError as c:
                print('Connection error occured.')
                if num_retries == MAX_RETRIES:
                    raise c
                else:
                    pass
            except ReadTimeout as t:
                print('Timeout occured')
                if num_retries == MAX_RETRIES:
                    raise t
                else:
                    pass
                pass
    except Exception as e:
        raise e

def _SendHelperData(reqId):
    try:
        helpers = _FindHelpers(reqId)
        res = postRequest(REMOTE_SERVER, PORT, 'v1/helpersData', helpers)
        return res.json()
        # TEST MARKER
        # res = {'partitionData': [
        #     {'addr': '127.0.0.1',
        #     'port': 9999,
        #     'fileName': 'download-0'},
        #     {'addr': '127.0.0.1',
        #     'port': 9998,
        #     'fileName': 'download-1'},
        #     {'addr': '127.0.0.1',
        #     'port': 9997,
        #     'fileName': 'download-2'}]}
        # return res
        
    except Exception as e:
        raise Exception('Error while finding helper node', e)
        # TODO send abort to server

def _BuildUrl(addr, port, path, protocol='http'):
    return protocol + '://' + addr + ':' + str(port) + ('/' if not path.startswith('/') else '') + path

def _FindHelpers(reqId):
    detectedHelpers = getRequest(REMOTE_SERVER, PORT, 'v1/getNodes').json()
    # detectedHelpers = [{'addr': '127.0.0.1',
    #                     'port': 9999},
    #                    {'addr': '127.0.0.1',
    #                     'port': 9998},
    #                    {'addr': '127.0.0.1',
    #                     'port': 9997}]
    print('Detected helpers: ', detectedHelpers)
    finalizedHelpers = []
    for helper in detectedHelpers:
        try:
            getRequest(helper['addr'], helper['port'], '/v1/healthCheck')
            finalizedHelpers.append(helper)
        except:
            print('Node ', helper['addr'] + ':' + str(helper['port']), 'did not respond. Removing from final helper list')
    data = {'requestId': reqId, 'helpers': finalizedHelpers}
    return data

def downloadChunk(partition, progressdict):
    chunkId = partition['reqId'] + '-' + str(partition['fileName'])
    try:
        # https://speed.hetzner.de/100MB.bin
        print('Starting download from', partition['src'], 'with chunk', partition['fileName'])
        file_name = 'chunks/' + chunkId
        link = partition['src']
        # link = 'https://speed.hetzner.de/100MB.bin'
        # link = 'http://speedtest.ftp.otenet.gr/files/test10Mb.db'
        with open(file_name, "wb") as f:
            response = requests.get(link, stream=True)
            total_length = response.headers.get('content-length')

            if total_length is None: # no content length header
                f.write(response.content)
            else:
                dl = 0
                total_length = int(total_length)
                for data in response.iter_content(chunk_size=8192):
                    dl += len(data)
                    f.write(data)
                    progress = int((dl*100)/total_length)
                    progressdict[chunkId] = progress
                    # sys.stdout.write('\rProgress: ' + str(progress))    
                    # sys.stdout.flush()
                    # done = int(50 * dl / total_length)
                    # sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50-done)) )    
                    # sys.stdout.flush()
        # print('Download complete')
    except Exception as e:
        print('Error occured while downloading the chunk', e)
        