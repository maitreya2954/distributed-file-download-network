import requests
import sys

def downloadChunk(partition, progressdict):
    chunkId = partition['reqId'] + '-' + str(partition['chunk'])
    try:
        # https://speed.hetzner.de/100MB.bin
        print('Starting download from', partition['src'], 'with chunk', partition['chunk'])
        file_name = 'chunks/' + chunkId
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
                    progress = int((dl*100)/total_length)
                    progressdict[chunkId] = progress
                    sys.stdout.write('\rProgress: ' + str(progress))    
                    sys.stdout.flush()
                    # done = int(50 * dl / total_length)
                    # sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50-done)) )    
                    # sys.stdout.flush()
        # print('Download complete')
    except Exception as e:
        print('Error occured while downloading the chunk', e)
        
def getProgress():
    return PROGRESS