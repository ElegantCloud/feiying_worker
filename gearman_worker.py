
# gearman_worker.py

import gearman
import json
import logging
import logging.handlers
import time
import oursql
import os
import sys
import urlparse
from optparse import OptionParser


def update_status(db, source_id, status):
    sql = """ UPDATE fy_video SET status=? WHERE source_id=? """ 
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
    return result

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

def download_episode(db, source_id, index, url, trackers, domain):
    update_episode_status(db, source_id, index, 2)
    vid = source_id + '_' + str(index) + '.mp4'
    r = download(vid, url, trackers, domain)
    if r != 0:
        update_episode_status(db, source_id, index, 4)
    else:
        update_episode_status(db, source_id, index, 100)

    return r

def download_multi(data, trackers, domain, db):
    source_id = data['source_id']
    el = get_episodes(source_id, db)
    if el == None:
        return 0
    
    for e in el:
        url = e[0]
        index = e[1]
        r = download_episode(db, source_id, index, url, trackers, domain)
        if r != 0:
            return (index, r)

    return len(el) 

def download_single(data, trackers, video_domain, image_domain, db):
    source_id = data['source_id']
    video_url = data['video_url']
    image_url = data['image_url']
    
    update_status(db, source_id, 2)

    pid = source_id + '.jpg'
    r = download(pid, image_url, trackers, image_domain)
    if r != 0:
        update_status(db, source_id, 3)
        return r

    vid = source_id + '.mp4'
    r = download(vid, video_url, trackers, video_domain)
    if r != 0:
        update_status(db, source_id, 4)
        return r
    
    update_status(db, source_id, 100)
    return 0

def func_video(worker, job, trackers, video_domain, image_domain, db, gmclient):
    data = json.loads(job.data)
    r = download_single(data, trackers, video_domain, image_domain, db) 
    if r == 0:
        req = gmclient.submit_job('fy_sphinx_index', job.data, wait_until_complete=False,
                background=True)
        gmclient.wait_until_jobs_accepted([req])

def func_movie(worker, job, trackers, video_domain, image_domain, db, gmclient):
    data = json.loads(job.data)
    r = download_single(data, trackers, video_domain, image_domain, db) 
    if r == 0:
        req = gmclient.submit_job('fy_sphinx_index', job.data, wait_until_complete=False,
                background=True)
        gmclient.wait_until_jobs_accepted([req])

def func_series(worker, job, trackers, video_domain, image_domain, db, gmclient):
    data = json.loads(job.data)
    source_id = data['source_id']
    image_url = data['image_url']

    update_status(db, source_id, 2) #begin download
    
    pid = source_id + '.jpg'
    r = download(pid, image_url, trackers, image_domain)
    if r != 0:
        update_status(db, source_id, 3) 
        return r

    r = download_multi(data, trackers, video_domain, db)
    if not isinstance(r, int):
        update_status(db, source_id, 4)
        return r

    update_status(db, source_id, 100) #download complete successfully

    if r == 0:
        req = gmclient.submit_job('fy_sphinx_index', job.data, wait_until_complete=False,
                background=True)
        gmclient.wait_until_jobs_accepted([req])

def func_updating_series(worker, job, trackers, video_domain, image_domain, db, gmclient):
    data = json.loads(job.data)
    source_id = data['source_id']
    
    update_status(db, source_id, 102) # 102 begin download new episodes
    r = download_multi(data, trackers, video_domain, db)
    if not isinstance(r, int):
        update_status(db, source_id, 104)
        return r
    else:
        update_status(db, source_id, 100) #download complete successfully
        count = data['episode_count']
        sql = "UPDATE fy_tv_series SET episode_count=? WHERE source_id=?"
        param = (count+r, source_id)
        with db.cursor() as cursor:
            cursor.execute(sql, param)
        return 0

class BaseWorker(object):

    name = 'fy_base_worker'

    def __init__(self, opts):
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.DEBUG)

        lh = logging.handlers.TimedRotatingFileHandler(self.name+'.log', when='midnight')
        lh.setLevel(logging.DEBUG)

        lf = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s : %(message)s')
        lh.setFormatter(lf)

        self.logger.addHandler(lh)

        self.opts = opts
        
        self.db = oursql.connect(
            host = opts.host,
            port = opts.port,
            user = opts.user,
            passwd = opts.pwd,
            db = opts.db)

        self.gmclient = gearman.client.GearmanClient([opts.gs])
        
        self.logger.info('create instance %s', self.name)

    def work(self):
        self.gmworker = gearman.GearmanWorker([self.opts.gs])
        self.gmworker.register_task(self.name, self.job_func(self))
        self.gmworker.work()

    @classmethod
    def job_func(cls, self):
        def f(w, j):
            self.do_work(w, j)
            return 'ok'
        return f

    def do_work(self, w, j):
        pass

    def _download_single(self, data):
        source_id = data['source_id']
        video_url = data['video_url']
        image_url = data['image_url']

        self._update_status(source_id, 2)

        pid = source_id + '.jpg'
        r = self._download(pid, image_url, self.opts.image_domain)
        if r != 0:
            self._update_status(source_id, 3)
            return r

        vid = source_id + '.mp4'
        r = self._download(vid, video_url, self.opts.video_domain)
        if r != 0:
            self._update_status(source_id, 4)
            return r
    
        self._update_status(source_id, 100)
        return 0

    def _download_multi(self, data, domain):
        source_id = data['source_id']
        el = self._get_episodes(source_id)
        if el == None:
            return 0
    
        for e in el:
            url = e[0]
            index = e[1]
            r = self._download_episode(source_id, index, url, domain)
            self.logger.info('download series %s episode %d result=%d', source_id, index, r)
            if r != 0:
                return (index, r)

        return len(el) 
    
    def _update_status(self, source_id, status):
        sql = """ UPDATE fy_video SET status=? WHERE source_id=? """ 
        param = (status, source_id)
        with self.db.cursor() as cursor:
            cursor.execute(sql, param)

    def _update_episode_status(self, source_id, index, status):
        sql = "UPDATE fy_tv_episode SET status=? WHERE source_id=? AND episode_index=?" 
        param = (status, source_id, index)
        with self.db.cursor() as cursor:
            cursor.execute(sql, param)

    def _download(self, fid, url, domain):
        parsed_url = urlparse.urlparse(url)
        query_list = urlparse.parse_qsl(parsed_url.query)
        curl_cmd = 'curl -G -L ' + parsed_url.scheme + '://' + parsed_url.netloc + parsed_url.path
        for q in query_list:
            curl_cmd += ' -d ' + q[0] + '=' + q[1]

        trackers = self.opts.trackers
        mogupload_cmd = "mogupload --trackers=%s --domain=%s --key='%s' --file='-'" % (trackers, domain, fid)

        cmd = curl_cmd + ' | ' + mogupload_cmd
        result = os.system(cmd)
        return result

    def _get_episodes(self, source_id):
        sql = """
            SELECT video_url, episode_index FROM fy_tv_episode WHERE source_id=? AND status=0 ORDER BY
            episode_index"""
        param = (source_id,)
        rl = None
        with self.db.cursor() as cursor:
            cursor.execute(sql, param)
            rl = cursor.fetchall()
        return rl 

    def _download_episode(source_id, index, url, domain):
        self._update_episode_status(source_id, index, 2)
        vid = source_id + '_' + str(index) + '.mp4'
        r = self._download(vid, url, domain)
        if r != 0:
            self._update_episode_status(source_id, index, 4)
        else:
            self._update_episode_status(source_id, index, 100)
        return r

class VideoWorker(BaseWorker):
    name = 'fy_video_download'
    def do_work(self, w, j):
        data = json.loads(j.data)
        r = self._download_single(data) 
        self.logger.info('download %s result=%d', data['source_id'], r)
        if r == 0:
            req = self.gmclient.submit_job('fy_sphinx_index', j.data, wait_until_complete=False,
                background=True)
            self.gmclient.wait_until_jobs_accepted([req])

class MovieWorker(VideoWorker):
    name = 'fy_movie_download'

class SeriesWorker(BaseWorker):
    name = 'fy_series_download'
    def do_work(self, w, j):
        data = json.loads(j.data)
        source_id = data['source_id']
        image_url = data['image_url']

        self.logger.info('download series %s', source_id)
        self._update_status(source_id, 2) #begin download
    
        pid = source_id + '.jpg'
        r = self._download(pid, image_url, self.opts.image_domain)
        if r != 0:
            self._update_status(source_id, 3) 
            self.logger.error('download %s image error %d', source_id, r)
            return r

        r = self._download_multi(data, self.opts.video_domain)
        if not isinstance(r, int):
            self._update_status(source_id, 4)
            return r

        self._update_status(source_id, 100) #download complete successfully

        if r == 0:
            req = self.gmclient.submit_job('fy_sphinx_index', j.data, wait_until_complete=False,
                    background=True)
            self.gmclient.wait_until_jobs_accepted([req])

class UpdatingSeriesWorker(BaseWorker):
    name = 'fy_updating_series_download'
    def do_work(self, w, j):
        data = json.loads(j.data)
        source_id = data['source_id']

        self.logger.info('download updating series %s', source_id)
    
        self._update_status(source_id, 102) # 102 begin download new episodes
        r = self._download_multi(data, self.opts.video_domain)
        if not isinstance(r, int):
            self._update_status(source_id, 104)
            return r
        else:
            self._update_status(source_id, 100) #download complete successfully
            count = data['episode_count']
            sql = "UPDATE fy_tv_series SET episode_count=? WHERE source_id=?"
            param = (count+r, source_id)
            with self.db.cursor() as cursor:
                cursor.execute(sql, param)
            return 0


def main():
    parser = OptionParser()
    parser.add_option('-g', '--gearman-servers', dest='gs', default='192.168.1.233:4730',
            help='gearman server list')
    parser.add_option('-w', '--worker-type', dest='worker', 
            help='worker type: (movie|series|useries|video)')
    parser.add_option('-m', '--mogilefs-trackers', dest='trackers', default='192.168.1.233:7001', 
            help='mogilefs trackers ---- ip:port[,ip:port]')
    parser.add_option('--mog-video-domain', dest='video_domain', default='testdomain',
            help='mogilefs domain for video files')
    parser.add_option('--mog-image-domain', dest='image_domain', default='testdomain',
            help='mogilefs domain for image files')
    
    parser.add_option('--db-host', dest='host', default='192.168.1.233',
            help='database host')
    parser.add_option('--db-port', dest='port', default=3306, type='int',
            help='database port')
    parser.add_option('--db-user', dest='user', default='futuom',
            help='database user')
    parser.add_option('--db-password', dest='pwd', default='ivyinfo123',
            help='database password')
    parser.add_option('--db-name', dest='db', default='feiying_new', 
            help='database name')

    (options, args) = parser.parse_args()

    if options.gs==None or options.trackers==None or options.worker==None or options.video_domain==None or options.image_domain==None: 
            parser.print_help()
            sys.exit()

    worker_dict = {'video':'VideoWorker', 
        'movie':'MovieWorker',
        'series':'SeriesWorker',
        'useries':'UpdatingSeriesWorker'
        }

    class_name = worker_dict[options.worker]
    if class_name == None:
        print 'The value of -w should be (movie|series|useries|video)'
        sys.exit()
    else:
        worker_constructor = globals()[class_name]
        worker = worker_constructor(options)
        worker.work()

if __name__ == '__main__':
    main()

