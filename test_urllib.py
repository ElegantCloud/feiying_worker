import urllib

# config urllib
class AppURLopener(urllib.FancyURLopener):
    version = 'Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11'

urllib._urlopener = AppURLopener()

url = 'http://g4.ykimg.com/0100641F464F72AA27BBA4046A6E93A5BB9839-7B1B-050A-DCA8-C6CF54DE8F90'
try:
    urllib.urlretrieve(url, 'test_file')
    result = 0
    print 'download result: %d ' % result
except IOError as (errno, strerr):
    print 'errno: {0} err info: {1}'.format(errno, strerr)
