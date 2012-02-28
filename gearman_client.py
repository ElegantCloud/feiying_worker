# gearman_client.py

import json
import gearman
import oursql
import sys
from optparse import OptionParser
from apscheduler.scheduler import Scheduler


class FeiyingGearmanClient(object):
    def __init__(self, host_list):
        self.admin_client = gearman.admin_client.GearmanAdminClient(host_list)
        self.client = gearman.client.GearmanClient(host_list)

    def _dump(self, li):
        for i in li:
            print i
            
    def dump_status(self):
        self._dump(self.admin_client.get_status())

    def dump_workers(self):
        self._dump(self.admin_client.get_workers())

    def get_queued_num(self, task_name):
        return self._get_property(task_name, 'queued')

    def get_running_num(self, task_name):
        return self._get_property(task_name, 'running')

    def _get_task(self, task_name):
        status = self.admin_client.get_status()
        for s in status:
            if s['task'] == task_name:
                return s

    def _get_property(self, task_name, prop_name):
        t = self._get_task(task_name)
        return t[prop_name] if t else None

    def submit_job(self, task):
        queued = self.get_queued_num(task.name)
        if None == queued:
            print 'ERROR: None == queued'
            return
        if queued >= task.max_queued:
            print 'WARNING: max queued'
            return

        datalist  = task.get_data_list(task.max_queued - queued)
        for data in datalist:
            print data
            self.client.submit_job(task.name, json.dumps(data), wait_until_complete=False,
                    background=True)


class FeiyingTask(object):
    def __init__(self, db, max_queued):
        self.db = db
        self.max_queued = max_queued

    def _query_db(self, sql, param):
        rl = None
        with self.db.cursor() as cursor:
            cursor.execute(sql, param)
            rl = cursor.fetchall()
        return rl

    def get_data_list(self, n):
        param = (n,) 
        rl = self._query_db(self.sql, param)
        dl = []
        if rl != None:
            for r in rl:
                dl.append(self._parse(r))
        return dl

    def _parse(self, r):
        pass


class SeriesTask(FeiyingTask):
    name = 'fy_series_download'
    sql = """
        SELECT v.source_id, v.title, v.image_url, s.release_date, s.origin, s.director, s.actor, s.episode_count
        FROM fy_video AS v RIGHT JOIN fy_tv_series AS s USING(source_id) WHERE v.category='series'
        AND v.status=0 AND s.episode_all=1 ORDER BY s.release_date DESC LIMIT ?"""

    def _parse(self, r):
        return {'source_id':r[0], 'title':r[1], 'image_url':r[2], 'release_date':r[3],
                'origin':r[4], 'director':r[5], 'actor':r[6], 'episode_count':r[7]}


class UpdatingSeriesTask(SeriesTask):
    name = 'fy_updating_series_download'
    sql = """
        SELECT v.source_id, v.title, v.image_url, s.release_date, s.origin, s.director, s.actor, s.episode_count
        FROM fy_video AS v RIGHT JOIN fy_tv_series AS s USING(source_id) WHERE v.category='series'
        AND v.status<2 AND s.episode_all=0 ORDER BY s.release_date DESC LIMIT ?"""

class MovieTask(FeiyingTask):
    name = 'fy_movie_download'
    sql = """
        SELECT v.source_id, v.title, v.image_url, m.video_url, m.release_date, m.origin, m.director, m.actor
        FROM fy_video AS v RIGHT JOIN fy_movie AS m USING(source_id) WHERE v.category='movie' AND v.status=0 
        ORDER BY m.release_date DESC LIMIT ?"""

    def _parse(self, r):
        return {'source_id':r[0], 'title':r[1], 'image_url':r[2], 'video_url':r[3], 'release_date':r[4],
                'origin':r[5], 'director':r[6], 'actor':r[7]}

def main():
    parser = OptionParser()
    parser.add_option('-g', '--gearman-servers', dest='gs', default='192.168.1.233:4730',
            help='gearman server list')
    parser.add_option('-t', '--task', dest='task', 
            help='gearman task name: (movie|series|useries)')
    parser.add_option('-n', '--num', dest='num', default=1, type='int',
            help='how many item to submit')

    parser.add_option('--status', dest='status_flag', action='store_true', default=False,
            help='show gearman server status')
    parser.add_option('--workers', dest='worker_flag', action='store_true', default=False,
            help='show gearman worker status')

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
    if options.gs==None:
        parser.print_help()
        sys.exit()

    gmclient = FeiyingGearmanClient([options.gs])

    if options.status_flag:
        gmclient.dump_status()
        sys.exit()

    if options.worker_flag:
        gmclient.dump_workers()
        sys.exit()

    if options.task == None:
        parser.print_help()
        sys.exit()

    db = oursql.connect(
            host = options.host,
            port = options.port,
            user = options.user,
            passwd = options.pwd,
            db = options.db)

    task = None
    if options.task == 'movie':
        task = MovieTask(db, options.num)
    elif options.task == 'series':
        task = SeriesTask(db, options.num)
    elif options.task == 'useries':
        task = UpdatingSeriesTask(db, options.num)
    else:
        parser.print_help()
        sys.exit()
    
    gmclient.submit_job(task)


if __name__ == '__main__':
    main()
