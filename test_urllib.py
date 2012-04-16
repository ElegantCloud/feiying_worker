import urllib

# config urllib
class AppURLopener(urllib.FancyURLopener):
    version = 'Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11'

urllib._urlopener = AppURLopener()

url = 'ihttp://m.tudou.com/down.do?cp=1169&code=113300919&codetype=2&encodeurl=aHR0cDovLzExMy4zMS4zNC4xNzo4MC93b3JrLzUxLzExMy8zMDAvOTE5LzUxLjIwMTIwNDA5MDYzNzQ1Lm1wNA=='
try:
    urllib.urlretrieve(url, 'test_file')
    result = 0
    print 'download result: %d ' % result
except IOError as (errno, strerr):
    print 'errno: {0} err info: {1}'.format(errno, strerr)
