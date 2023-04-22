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
import threading

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
    

def _MonitorProgress(partitionData, partitiondict, requestId):
    try:
        partitionInfo = partitionData['partitionData']
        partitiondict[requestId] = partitionData
        print('Progress monitor started')
        
        while not downloadComplete(partitionInfo):  
            printstring = '' 
            for partition in partitionInfo:
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
            partitiondict[requestId] = partitionData
            time.sleep(2)
        print('\n>>> Chunks downloaded on all helpers. Starting gathering chunks')
        return partitionInfo
    except Exception as e:
        raise e
    
def _DownloadChunk(partition, chunkId, downloadDir):
    print('Getting chunk: ', chunkId)
    response = getRequest(partition['addr'], partition['port'], '/v1/chunk/' + chunkId)
    with open(downloadDir + '/' + partition['fileName'], 'wb') as f:
        f.write(response.content)
    print(chunkId, 'received')
        
def _GatherChunks(requestId, partitionInfo):
    try:
        downloadDir = 'downloads/' + requestId + '/chunks'
        os.makedirs(downloadDir)
        downloadthreads = []
        for partition in partitionInfo['partitionData']:
            chunkId = partition['reqId'] + '-' + str(partition['fileName'])
            if partition['admin']:
                shutil.move('chunks/'+chunkId, downloadDir + '/' + partition['fileName'])
            else:
                downloadthreads.append(threading.Thread(target=_DownloadChunk, name=chunkId, args=[partition, chunkId, downloadDir]))
                # print('Getting chunk: ', chunkId)
                # response = getRequest(partition['addr'], partition['port'], '/v1/chunk/' + chunkId)
                # with open(downloadDir + '/' + partition['fileName'], 'wb') as f:
                #     f.write(response.content)
                # print(chunkId, 'received')
        for i in downloadthreads:
            i.start()
        for i in downloadthreads:
            i.join()
        print('Merging files')
        open(downloadDir + '/manifest', "w").write(partitionInfo['manifest'])
        merge = Merge(downloadDir, 'downloads/' + requestId, partitionInfo['originalName'])
        merge.merge(cleanup=True)
        with open('downloads/' + requestId + '/' + partitionInfo['originalName'],"rb") as f:
            bytes = f.read()
            if hashlib.md5(bytes).hexdigest() == partitionInfo['checksum']:
                print('Merge successfully completed')
                # os.system('open downloads/' + requestId + '/' + partitionInfo['originalName'])
            else:
                raise Exception('File corrupted')
    except Exception as e:
        raise e
    
def initiateDownload(requestId, partitiondict):
    try:
        start=time.time()
        partitionData = _SendHelperData(requestId)
        partitionData['status'] = 1
        partitiondict[requestId] = partitionData
        _SendPartitionData(partitionData, requestId)
        partitionData['status'] = 2
        partitiondict[requestId] = partitionData
        _MonitorProgress(partitionData, partitiondict, requestId)
        # partitiondict[requestId]['status'] = 3
        partitionData['status'] = 3
        partitiondict[requestId] = partitionData
        _GatherChunks(requestId, partitionData)
        partitionData['status'] = 4
        partitiondict[requestId] = partitionData
        print('Time taken to download: ', time.time()-start)
    except Exception as e:
        print('Exception while initiating download', e)
        traceback.print_exc()
    
def _SendPartitionData(partitionData, requestId):
    print('PartitionData from server: ', partitionData)
    if 'partitionData' in partitionData:
        try:
            admin=True
            for partition in partitionData['partitionData']:
                partition['admin'] = admin
                admin = False
                partition['src'] = _BuildUrl(REMOTE_SERVER, PORT, 'v1/chunk/' + requestId + '/' + partition['fileName'])
                partition['reqId'] = requestId
                partition['progress'] = 0
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
    print('Final helpers: ', finalizedHelpers)
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
        