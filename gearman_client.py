# gearman_client.py

import gearman


class AdminClient(object):
    def __init__(self, host_list):
        self.client = gearman.admin_client.GearmanAdminClient(host_list)

    def get_queued_num(self, task_name):
        return self._get_property(task_name, 'queued')

    def get_running_num(self, task_name):
        return self._get_property(task_name, 'running')

    def _get_task(self, task_name):
        status = self.client.get_status()
        for s in status:
            if s['task'] == task_name:
                return s

    def _get_property(self, task_name, prop_name):
        t = self._get_task(task_name)
        return t[prop_name] if t else None


class SeriesClient(object):
    def __init__(self):
        pass

    def run(self):


admclient = AdminClient(['192.168.1.233'])
task = 'fy_download_and_index_series_updating'
print admclient.get_queued_num(task)
print admclient.get_running_num(task)

