# crawl_scheduler.py

import gearman
import os
import os.path
import sys
from optparse import OptionParser
from apscheduler.scheduler import Scheduler

def main():
    parser = OptionParser()
    parser.add_option('--scrapyd', dest='scrapyd', default='scrapy-server:6800',
            help='scrapyd url')
    parser.add_option('--project', dest='project', default='feiying',
            help='scrapy project name')

    (options, args) = parser.parse_args()
    if options.scrapyd == None or options.project == None:
        parser.print_help()
        sys.exit()

    cmd = "curl http://%s/schedule.json -d project=%s -d spider=" % (options.scrapyd,
            options.project)

    sched = Scheduler()
    sched.daemonic = False

    @sched.cron_schedule(hour='0-23/2', minute=20)
    def crawl_youku_video():
        os.system(cmd + 'youku_video')

    @sched.cron_schedule(hour='0-23/2', minute=30)
    def crawl_tudou_video():
        os.system(cmd + 'tudou_video')

    @sched.cron_schedule(hour=11, minute=25)
    def crawl_letv_movie():
        os.system(cmd + 'letv_movie')
             
    @sched.cron_schedule(hour=21, minute=25)
    def crawl_letv_series():
        os.system(cmd + 'letv_series')
   
    # schedule the task of feiying mysql index backup in coreseek
    @sched.interval_schedule(hours=3)
    def backup_feiying_mysql_index():
        bak_path = '/backups'
        is_path_ready = True
        if os.path.exists(bak_path) == False:
            # mkdir for backup path
            try:
                os.makedirs(bak_path)
                print 'create ' + bak_path + ' successfully'
            except error:
                is_path_ready = False
                print 'create ' + bak_path + " error"
        # do backup
        bak_file_name = "feiying_bak.tar.gz"
        tmp_bak_name = "tmp_" + bak_file_name
        coreseek_data_path = "/usr/local/coreseek/var/data" 
        if is_path_ready == True:
            backup_cmd = "tar -zcvf " + bak_path + "/" + tmp_bak_name + " " + coreseek_data_path  + "/feiying_mysql.* " + coreseek_data_path + "/binlog.*"
            result = os.system(backup_cmd)
            if 0 == result:
                # delete previous backup file, and rename tmp backup file to formal backup file
                os.rename(bak_path + "/" + tmp_bak_name, bak_path + "/" + bak_file_name)
    
    # Schedule the task of feiying mysql index
    @sched.interval_schedule(hours=2)
    def index_feiying_mysql():
        index_cmd = "/usr/local/coreseek/bin/indexer -c /usr/local/coreseek/etc/feiying_mysql.conf --all --rotate --quiet"
        os.system(index_cmd)
    
    sched.start()

if __name__ == '__main__':
    main()
