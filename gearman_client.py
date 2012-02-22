# gearman_client.py

import json
import gearman
import oursql


class FeiyingGearmanClient(object):
    def __init__(self, host_list):
        self.admin_client = gearman.admin_client.GearmanAdminClient(host_list)
        self.client = gearman.client.GearmanClient(host_list)

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
        queued = self.admin_client.get_queued_num(task.name)
        if None == queued:
            return
        if queued >= task.max_queued:
            return

        datalist  = task.get_data_list(task.max_queued - queued)
        for data in datalist:
            self.client.submit_job(task.name, json.dumps(date), wait_until_complete=False)



class FeiyingTask(object):
    def __init__(self):
        self.db = oursql.connect(
            host='192.168.1.233',
            user='futuom',
            passwd='ivyinfo123',
            db='feiying')

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
    max_queued = 5
    sql = """
        SELECT source_id, title FROM fy_tv_series WHERE status=0 AND episode_all=1 ORDER BY
        release_date DESC LIMIT ?"""

    def _parse(self, r):
        return {'source_id':r[0], 'title':r[1]}


class UpdatingSeriesTask(SeriesTask):
    name = 'fy_updating_series_download'
    max_queued = 5
    sql = """
        SELECT source_id, title FROM fy_tv_series WHERE status<2 AND episode_all=0 ORDER BY
        release_date DESC LIMIT ?"""

class MovieTask(FeiyingTask):
    name = 'fy_movie_download'
    max_queued = 20
    sql = """
        SELECT source_id, title, video_url FROM fy_movie WHERE status=0 ORDER BY release_date DESC
        LIMIT ?"""

    def _parse(self, r):
        return {'source_id':r[0], 'title':r[1], 'video_url':r[2]}


def test():
    gmclient = FeiyingGearmanClient(['192.168.1.233'])
    task = 'fy_download_and_index_series_updating'
    print gmclient.get_queued_num(task)
    print gmclient.get_running_num(task)

    st = SeriesTask()
    dl = st.get_data_list(10)
    for d in dl:
        print json.dumps(d)

    print '-------'

    ust = UpdatingSeriesTask()
    udl = ust.get_data_list(10)
    for ud in udl:
        print json.dumps(ud)

    print '-------'

    mt = MovieTask()
    mdl = mt.get_data_list(10)
    for md in mdl:
        print json.dumps(md)

if __name__ == '__main__':
    test()
