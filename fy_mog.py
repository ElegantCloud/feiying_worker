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

class ListAction(Action):
    def doAction(self):
        l = self.client.list_keys(self.options.source_id)
        print l
        return l

class DeleteAction(Action):
    def doAction(self):
        #delete form database
        self.db = oursql.connect(
            host = options.host,
            port = options.port,
            user = options.user,
            passwd = options.pwd,
            db = options.db)
  
        sql = "delete from fy_video where source_id=?"
        params = (self.options.source_id, )
        with self.db.cursor() as cursor:
            cursor.execute(sql, params)
        
        #delete files of this source_id, include mp4, jpg, m3u8, ts
        keylist = self.client.list_keys(self.options.source_id) 
        print 'delete files in:', keylist
        for k in keylist:
            self.client.delete(k)

def main():
    parser = OptionParser()
    parser.add_option('--action', dest='action', help='action: list|delete')
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
    if options.action==None or options.source_id==None:
        parser.print_help()
        sys.exit()

    if options.action == 'list':
        action = ListAction(options)
    else:
        parser.print_help()
        sys.exit()

    action.doAction();


if __name__ == '__main__':
    main()
