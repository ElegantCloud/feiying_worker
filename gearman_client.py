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
            print '-------'
            task.update_status(data)
            job = self.client.submit_job(task.name, json.dumps(data), wait_until_complete=False,
                    background=True)
            self.client.wait_until_jobs_accepted([job])


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

    def update_status(self, data):
        source_id = data['source_id']
        sql = """
            UPDATE fy_video SET status=? WHERE source_id=?"""
        param = (self.pending_status, source_id)
        with self.db.cursor() as cursor:
            cursor.execute(sql, param)


class SeriesTask(FeiyingTask):
    name = 'fy_series_download'
    pending_status = 1
    sql = """
        SELECT v.source_id, v.title, v.image_url, s.release_date, s.origin, s.director, s.actor, s.episode_count
        FROM fy_video AS v RIGHT JOIN fy_tv_series AS s USING(source_id) WHERE v.channel=2 AND v.status=0 
        ORDER BY v.created_time DESC LIMIT ?"""

    def _parse(self, r):
        return {'source_id':r[0], 'title':r[1], 'image_url':r[2], 'release_date':r[3],
                'origin':r[4], 'director':r[5], 'actor':r[6], 'episode_count':r[7]}

class UpdatingSeriesTask(SeriesTask):
    name = 'fy_updating_series_download'
    pending_status = 101
    sql = """
        SELECT v.source_id, s.episode_count, s.episode_all
        FROM fy_video AS v 
        RIGHT JOIN fy_tv_series AS s USING(source_id)
        LEFT JOIN fy_tv_episode AS e USING(source_id)
        WHERE v.channel=2 AND v.status=200 AND s.episode_all=0 AND e.status=0
        GROUP BY v.source_id
        ORDER BY v.created_time DESC LIMIT ?"""

    def _parse(self, r):
        return {'source_id':r[0], 'episode_count':r[1], 'episode_all':r[2]}

class MovieTask(FeiyingTask):
    name = 'fy_movie_download'
    pending_status = 1
    sql = """
        SELECT v.source_id, v.title, v.image_url, m.video_url, m.release_date, m.origin, m.director, m.actor
        FROM fy_video AS v RIGHT JOIN fy_movie AS m USING(source_id) WHERE v.channel=1 AND v.status=0 
        ORDER BY v.created_time DESC LIMIT ?"""

    def _parse(self, r):
        return {'source_id':r[0], 'title':r[1], 'image_url':r[2], 'video_url':r[3], 'release_date':r[4],
                'origin':r[5], 'director':r[6], 'actor':r[7]}

def schedule(gmclient, db, num):
    sched = Scheduler()
    sched.daemonic = False

    @sched.cron_schedule(minute=00)
    def movie_task():
        task = MovieTask(db, num)
        gmclient.submit_job(task)
     
    @sched.cron_schedule(minute=10)
    def series_task():
        task = SeriesTask(db, num)
        gmclient.submit_job(task)
    
    @sched.cron_schedule(minute=40)
    def useries_task():
        task = UpdatingSeriesTask(db, num)
        gmclient.submit_job(task)

    sched.start()


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
    parser.add_option('--db-name', dest='db', default='feiying_new',
            help='database name')

    parser.add_option('--schedule', dest='schedule_flag', action='store_true', default=False,
            help='run client as scheduler')

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

    db = oursql.connect(
            host = options.host,
            port = options.port,
            user = options.user,
            passwd = options.pwd,
            db = options.db)

    if options.schedule_flag:
        schedule(gmclient, db, options.num)
        sys.exit()

    if options.task == None:
        parser.print_help()
        sys.exit()

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
