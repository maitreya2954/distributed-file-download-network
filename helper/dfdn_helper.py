import requests
import sys
import traceback


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

            if total_length is None:  # no content length header
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
        traceback.print_exc()


def postRequest(addr, port, path, data, protocol='http'):
    try:
        url = _BuildUrl(addr, port, path, protocol)
        print('Post request :', url)
        res = requests.post(url, json=data)
        if res.status_code != 200:
            raise Exception('Request failed. Status code ' + str(res.status_code) + ' received - ' + res.reason)
    except Exception as e:
        raise e
    return res

def _BuildUrl(addr, port, path, protocol='http'):
    return protocol + '://' + addr + ':' + str(port) + ('/' if not path.startswith('/') else '') + path