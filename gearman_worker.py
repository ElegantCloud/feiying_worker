# gearman_worker.py

import gearman
import json
import time
import oursql
import os
import sys
import urlparse
from optparse import OptionParser

def update_status(db, table, source_id, status):
    sql = """ UPDATE %s SET status=? WHERE source_id=? """ % (table,)
    param = (status, source_id)
    with db.cursor() as cursor:
        cursor.execute(sql, param)

def update_episode_status(db, source_id, index, status):
    sql = "UPDATE fy_tv_episode SET status=? WHERE source_id=? AND episode_index=?" 
    param = (status, source_id, index)
    with db.cursor() as cursor:
        cursor.execute(sql, param)

def download(vid, url, trackers, domain):
    parsed_url = urlparse.urlparse(url)
    query_list = urlparse.parse_qsl(parsed_url.query)
    curl_cmd = 'curl -G -L ' + parsed_url.scheme + '://' + parsed_url.netloc + parsed_url.path
    for q in query_list:
        curl_cmd += ' -d ' + q[0] + '=' + q[1]

    mogupload_cmd = "mogupload --trackers=%s --domain=%s --key='%s' --file='-'" % (trackers, domain, vid)

    cmd = curl_cmd + ' | ' + mogupload_cmd
    result = os.system(cmd)
    

def get_episodes(source_id, db):
    sql = """
        SELECT video_url, episode_index FROM fy_tv_episode WHERE source_id=? AND status=0 ORDER BY
        episode_index"""
    param = (source_id,)
    rl = None
    with db.cursor() as cursor:
        cursor.execute(sql, param)
        rl = cursor.fetchall()
    return rl 

def download_episodes(db, source_id, el, trackers, domain):
    for e in el:
        url = e[0]
        index = e[1]
        update_episode_status(db, source_id, index, 1)
        vid = source_id + '_' + str(index) + '.mp4'
        download(vid, url, trackers, domain)
        update_episode_status(db, source_id, index, 2)

def download_series(worker, job, trackers, domain, db):
    data = json.loads(job.data)
    source_id = data['source_id']
    el = get_episodes(source_id, db)
    if el == None:
        return
    update_status(db, 'fy_tv_series', source_id, 1) 
    download_episodes(db, source_id, el, trackers, domain)
    update_status(db, 'fy_tv_series', source_id, 2) 

def download_updating_series(worker, job, trackers, domain, db):
    pass

def download_movie(worker, job, trackers, domain, db):
    pass

def worker_wrapper(func, trackers, domain, db):
    def f(w, j):
        func(w, j, trackers, domain, db)
    return f
    

def main():
    parser = OptionParser()
    parser.add_option('-g', '--gearman-servers', dest='gs', default='192.168.1.233:4730',
            help='gearman server list')
    parser.add_option('-w', '--worker-type', dest='worker', 
            help='worker type: (movie|series|useries)')
    parser.add_option('-m', '--mogilefs-trackers', dest='trackers', default='192.168.1.233:7001', 
            help='mogilefs trackers ---- ip:port[,ip:port]')
    parser.add_option('-d', '--mogilefs-domain', dest='domain', default='testdomain',
            help='mogilefs domain')
    
    parser.add_option('--db-host', dest='host', default='192.168.1.233',
            help='database host')
    parser.add_option('--db-port', dest='port', default=3306, type='int',
            help='database port')
    parser.add_option('--db-user', dest='user', default='futuom',
            help='database user')
    parser.add_option('--db-password', dest='pwd', default='ivyinfo123',
            help='database password')
    parser.add_option('--db-name', dest='db', default='feiying', 
            help='database name')

    (options, args) = parser.parse_args()

    if options.gs==None or options.trackers==None or options.worker==None or options.domain==None: 
        parser.print_help()
        sys.exit()

    name_dict = {
            'movie':{'name':'fy_movie_download', 'func':download_movie},
            'series':{'name':'fy_series_download', 'func':download_series},
            'useries':{'name':'fy_updating_series_download', 'func':download_updating_series}
        }
    
    item = name_dict[options.worker]
    if item == None:
        print 'The value of -w should be (movie|series|useries)'
        sys.exit()

    db = oursql.connect(
            host = options.host,
            port = options.port,
            user = options.user,
            passwd = options.pwd,
            db = options.db)

    gearman_worker = gearman.GearmanWorker([options.gs])
    gearman_worker.set_client_id(str(time.time()))
    gearman_worker.register_task(item['name'], 
            worker_wrapper(item['func'], options.trackers, options.domain, db))
    gearman_worker.work()

if __name__ == '__main__':
    main()

