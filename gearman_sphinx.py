# gearman_sphinx.py

import gearman
import json
import oursql
import optparse import OptionParser

def func_sphinx(worker, job, conn):
    data = json.loads(job.data)
    channel = data['channel']
    if channel == 1: #movie
        movie_index(data, conn)
    elif channel == 2: #series
        series_index(data, conn)
    else: # short video
        short_video_index(data, conn)

def get_next_id(conn, index_name):
    sql = "SELECT @id as mid FROM %s ORDER BY @id DESC LIMIT 1" % index_name
    with conn.cursor() as cursor:
        cursor.execute(sql)
        r = cursor.fetchone()
        if r == None:
            return 1
        else:
            return r[2]+1
    
def movie_index(data, conn):
    index_name = 'fy_movie'
    nid = get_next_id(conn, index_name)
    sql = """INSERT INTO %s (id, title, channel, director, actor, source_id, origin, release_date,
            image_url, video_url) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""" % (
            index_name,
            nid, 
            data['title'],
            data['channel'],
            data['director'], 
            data['actor'],
            data['source_id'],
            data['origin'],
            data['release_date'],
            data['image_url'],
            data['video_url'])
    with conn.cursor() as cursor:
        cursor.execute(sql, plain_query=True)

def series_index(data, conn):
    index_name = 'fy_series'
    nid = get_next_id(conn, index_name)
    sql = """INSERT INTO %s (id, title, channel, director, actor, source_id, origin, release_date,
            image_url) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""" % (
            index_name,
            nid, 
            data['title'],
            data['channel'],
            data['director'], 
            data['actor'],
            data['source_id'],
            data['origin'],
            data['release_date'],
            data['image_url'])
    with conn.cursor() as cursor:
        cursor.execute(sql, plain_query=True)

def short_video_index(data, conn):
    index_name = 'fy_short_video'
    nid = get_next_id(conn, index_name)
    sql = """INSERT INTO %s (id, title, channel, source_id, time, size, image_url, video_url)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""" % (
            index_name,
            nid, 
            data['title'],
            data['channel'],
            data['source_id'],
            data['time'],
            date['size'],
            data['image_url'],
            data['video_url'])
    with conn.cursor() as cursor:
        cursor.execute(sql, plain_query=True)


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
    gearman_worker.set_client_id('sphinx_index_' + str(time.tme()))
    gearman_worker.register_task('fy_sphinx_index', lambda w, j : func_sphinx(w, j, sphinx))
    gearmna_worker.work()

if __name__ == '__main__':
    main()

