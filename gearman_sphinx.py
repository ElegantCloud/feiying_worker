# gearman_sphinx.py

import gearman
import json
import oursql
import time
from optparse import OptionParser


def get_next_id(conn, index_name):
    sql = "SELECT @id as mid FROM %s ORDER BY @id DESC LIMIT 1" % index_name
    with conn.cursor() as cursor:
        cursor.execute(sql, plain_query=True)
        r = cursor.fetchone()
        if r == None:
            return 1
        else:
            return r[2]+1
    
def movie_index(data, conn):
    index_name = 'fy_movie'
    nid = get_next_id(conn, index_name)
    sql = """INSERT INTO %s (id, channel, title, director, actor, source_id, origin, release_date,
            image_url, video_url) VALUES (%d, %d,'%s','%s','%s','%s','%s','%s','%s','%s')""" % (
            index_name,
            nid, 
            data['channel'],
            data['title'],
            data['director'], 
            data['actor'],
            data['source_id'],
            data['origin'],
            data['release_date'],
            data['image_url'],
            data['video_url'])
    with conn.cursor() as cursor:
        cursor.execute(sql, plain_query=True)
        return 0
    return 1

def series_index(data, conn):
    index_name = 'fy_series'
    nid = get_next_id(conn, index_name)
    sql = """INSERT INTO %s (id, channel, title, director, actor, source_id, origin, release_date,
            image_url) VALUES (%d, %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
            index_name,
            nid, 
            data['channel'],
            data['title'],
            data['director'], 
            data['actor'],
            data['source_id'],
            data['origin'],
            data['release_date'],
            data['image_url'])
    with conn.cursor() as cursor:
        cursor.execute(sql, plain_query=True)
        return 0
    return 1

def short_video_index(data, conn):
    index_name = 'fy_short_video'
    nid = get_next_id(conn, index_name)
    sql = """INSERT INTO %s (id, channel, title, source_id, time, size, image_url, video_url)
            VALUES (%d, %d, '%s', '%s', '%s', '%s', '%s', '%s')""" % (
            index_name,
            nid, 
            data['channel'],
            data['title'],
            data['source_id'],
            data['time'],
            date['size'],
            data['image_url'],
            data['video_url'])
    with conn.cursor() as cursor:
        cursor.execute(sql, plain_query=True)
        return 0
    return 1


def func_sphinx(worker, job, conn):
    data = json.loads(job.data)
    channel = data['channel']
    if channel == 1: #movie
        return movie_index(data, conn)
    elif channel == 2: #series
        return series_index(data, conn)
    else: # short video
        return short_video_index(data, conn)

def func_wrapper(conn, func):
    def f(worker, job):
        return str(func(worker, job, conn))
    return f


def main():
    parser = OptionParser()
    parser.add_option('-g', '--gearman-servers', dest='gs', default='192.168.1.233:4730',
            help='gearman server list')
    
    parser.add_option('--sphinx-host', dest='host', default='192.168.1.113',
            help='sphinx host')
    parser.add_option('--sphinx-port', dest='port', default=9306, type='int',
            help='sphinx port')

    (options, args) = parser.parse_args()
    if options.gs==None or options.host==None or options.port==None:
        parser.print_help()
        sys.exit()

    sphinx = oursql.connect(host=options.host, port=options.port, charset='utf8')
    gearman_worker = gearman.GearmanWorker([options.gs])
    gearman_worker.set_client_id('sphinx_index_' + str(time.time()))
    gearman_worker.register_task('fy_sphinx_index', func_wrapper(sphinx, func_sphinx))
    gearman_worker.work()

if __name__ == '__main__':
    main()

