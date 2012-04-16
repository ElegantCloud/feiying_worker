from urlgrabber import urlgrab
url = 'http://i1.letvimg.com/vrs/201204/05/c3671b2ca6be47c6bcdb4d32e24f60ab.jpg'

try:
    urlgrab(url, '/tmp/' + 'image')
    print 'download ok'
except Exception as (errno, strerr):
    print('download failed - ERRNO: %d ERR INFO: %s ' % (errno, strerr))
