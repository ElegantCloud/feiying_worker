# gearman_client.py
# schedule the task of movie, series, useries downloading

import sys
import oursql
from optparse import OptionParser
import mogilefs

class Action(object):
    def __init__(self, options):
        self.options = options
        self.client = mogilefs.client.Client(domain=self.options.domain, hosts=self.options.trackers.split(',')) 

    def _setup_db(self):
        self.db = oursql.connect(
            host = self.options.host,
            port = self.options.port,
            user = self.options.user,
            passwd = self.options.pwd,
            db = self.options.db)

class ListAction(Action):
    def doAction(self):
        try:
            keylist = self.client.list_keys(self.options.source_id)
            for k in keylist:
                print k
        except Exception as e:
                print e

class DeleteAction(Action):
    def doAction(self):
        print "Delete %s" % self.options.source_id
        #delete form database
        self._setup_db()
        print "Delete %s from database." % self.options.source_id
        sql = "delete from fy_video where source_id=?"
        params = (self.options.source_id, )
        with self.db.cursor() as cursor:
            cursor.execute(sql, params)
        
        #delete files of this source_id, include mp4, jpg, m3u8, ts
        try:
            keylist = self.client.list_keys(self.options.source_id) 
	    for k in keylist:
                print "Delete %s from MogileFS." % k
	        self.client.delete(k)
        except Exception as e:
            print e

class CheckAction(Action):
    def doAction(self):
        self._setup_db()
        sql = "select source_id, status, created_time from fy_video where channel>2 and status<>100 order by created_time limit 100"
        with self.db.cursor() as cursor:
            cursor.execute(sql)
            result_list = cursor.fetchall()
        for r in result_list:
            self.options.source_id = r[0]
            d = DeleteAction(self.options)
            d.doAction()

class Checkm3u8Action(Action):
    def doAction(self):
        self._setup_db()
        sql = """select source_id, status, created_time from fy_video 
                 where channel>2 and fav_count=0 and share_count=0 
                 order by created_time limit 1000"""
        with self.db.cursor() as cursor:
            cursor.execute(sql)
            result_list = cursor.fetchall()
        for r in result_list:
            paths = self.client.get_paths(r[0]+".m3u8") 
            if len(paths)<=0:
                self.options.source_id = r[0]
                d = DeleteAction(self.options)
                d.doAction()
            

def main():
    parser = OptionParser()
    parser.add_option('--action', dest='action', help='action: list|delete|check|checkm3u8')
    parser.add_option('--source_id', dest='source_id', help='source_id of media file')

    parser.add_option('--trackers', dest='trackers', default='127.0.0.1:7001', help='MogileFS trackers')
    parser.add_option('--domain', dest='domain', default='fydomain', help='MogileFS Domain')

    parser.add_option('--db-host', dest='host', default='mysql-server',
            help='database host')
    parser.add_option('--db-port', dest='port', default=3306, type='int',
            help='database port')
    parser.add_option('--db-user', dest='user', default='feiying',
            help='database user')
    parser.add_option('--db-password', dest='pwd', default='feiying123',
            help='database password')
    parser.add_option('--db-name', dest='db', default='feiying',
            help='database name')

    (options, args) = parser.parse_args()
    if options.action==None:
        parser.print_help()
        sys.exit()

    if options.action == 'list':
        action = ListAction(options)
    elif options.action == 'delete':
        action = DeleteAction(options)
    elif options.action == 'check':
        action = CheckAction(options)
    elif options.action == 'checkm3u8':
        action = Checkm3u8Action(options)
    else:
        parser.print_help()
        sys.exit()

    action.doAction();


if __name__ == '__main__':
    main()
