import requests

def startDownload(partition):
    print('Starting download from', partition['remote'], 'with chunk', partition['part'])
    # r = requests.get(url, allow_redirects=True)
    # open(filename, 'wb').write(r.content)