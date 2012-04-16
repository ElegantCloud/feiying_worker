from urlgrabber import urlgrab
url = 'http://m.letv.com/playvideo.php?id=1530149&mmsid=1742836&cid=0&type=2&br=350'

try:
    filename = urlgrab(url, 'video')
    print filename
except Exception as (errno, strerr):
    print('download failed - ERRNO: %d ERR INFO: %s ' % (errno, strerr))
