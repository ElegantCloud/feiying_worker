# crawl_scheduler.py

import gearman
import os
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

    sched.start()

if __name__ == '__main__':
    main()
