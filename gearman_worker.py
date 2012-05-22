
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
import urllib
import socket

# timeout for socket - 60 seconds 
socket.setdefaulttimeout(60.0)

# config urllib
class AppURLopener(urllib.FancyURLopener):
    version = 'Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11'
urllib._urlopener = AppURLopener()


class BaseWorker(object):

    name = 'fy_base_worker'

    def __init__(self, opts):
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.DEBUG)

        lh = logging.handlers.TimedRotatingFileHandler('/tmp/'+self.name+'.log', when='midnight', backupCount=10)
        lh.setLevel(logging.DEBUG)

        lf = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s : %(message)s')
        lh.setFormatter(lf)

        self.logger.addHandler(lh)

        self.opts = opts
       
        self.gm_host_list = opts.gs.split(',')
        self.gmclient = gearman.client.GearmanClient(self.gm_host_list)
        
        self.logger.info('create instance %s', self.name)

    def work(self):
        self.gmworker = gearman.GearmanWorker(self.gm_host_list)
        self.gmworker.register_task(self.name, self.job_func(self))
        self.gmworker.work()

    @classmethod
    def job_func(cls, self):
        def f(w, j):
            self._base_do_work(w, j)
            return 'ok'
        return f

    def _base_do_work(self, w, j):
	self.db = oursql.connect(
            host = self.opts.host,
            port = self.opts.port,
            user = self.opts.user,
            passwd = self.opts.pwd,
            db = self.opts.db)
	self.do_work(w, j)	
	self.db.close()

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
            self.logger.info('series %s has not any episodes for download', source_id)
            return -1
    
        self.logger.info('series %s has %d episodes for download', source_id, len(el))
        result = 0
        for e in el:
            url = e[0]
            index = e[1]
            r = self._download_episode(source_id, index, url, domain)
            self.logger.info('download series %s episode %d result=%d', source_id, index, r)
            if r != 0:
                result += 1
            else: # udpate episode_count
                self._update_episode_count(source_id, index)

        return result
    
    def _update_status(self, source_id, status):
        self.logger.info('update status - source id: %s status: %d ', source_id, status)
        sql = """ UPDATE fy_video SET status=? WHERE source_id=? """ 
        param = (status, source_id)
        with self.db.cursor() as cursor:
            cursor.execute(sql, param)

    def _update_episode_count(self, source_id, count):
        sql = "UPDATE fy_tv_series SET episode_count=? WHERE source_id=? AND episode_count<?"
        param = (count, source_id, count)
        with self.db.cursor() as cursor:
            cursor.execute(sql, param)

    def _update_episode_status(self, source_id, index, status):
        sql = "UPDATE fy_tv_episode SET status=? WHERE source_id=? AND episode_index=?" 
        param = (status, source_id, index)
        with self.db.cursor() as cursor:
            cursor.execute(sql, param)

    def _download(self, fid, url, domain):
        self.logger.info('======  download %s begin  ======', fid)
        #parsed_url = urlparse.urlparse(url)
        #query_list = urlparse.parse_qsl(parsed_url.query)
        prefix, ext = fid.split(".")
        tmp_file_path = '/tmp/'
        tmp_file = tmp_file_path + "tmp_" + fid

        #video_url = parsed_url.scheme + '://' + parsed_url.netloc + parsed_url.path
        #curl_cmd = 'curl -G -L ' + parsed_url.scheme + '://' + parsed_url.netloc + parsed_url.path + " -o " + tmp_file_path + "tmp_" + fid 
        #for q in query_list:
        #    curl_cmd += ' -d ' + q[0] + '=' + q[1]
        #self.logger.info('# downloading - cmd: ' + curl_cmd)
        #result = os.system(curl_cmd)    # download video file and save as tmp file
        
        try:
            self.logger.info('# downloading %s - url: %s', fid, url)
            urllib.urlretrieve(url, tmp_file) 
            self.logger.info('# download ok')
        except IOError as (errtype, strerr):
            self.logger.error('# download failed - err type: {0} - err info: {1}'.format(errtype, strerr))
            return -1
        except:
            self.logger.error('# socket timeout for download')
            return -2
        
        result = 0
        # sgement mp4 to MPEG-TS files 
        if ".mp4" == ext:
            tmp_dir = tmp_file_path + prefix + '/'
            os.makedirs(tmp_dir)
            ffmpeg_segment_cmd = "ffmpeg -i %s -f segment -segment_time 10 -segment_format \
                mpegts -codec copy -bsf:v h264_mp4toannexb -map 0 %s" % (tmp_file, tmp_dir+prefix)
            ffmpeg_segment_cmd = ffmpeg_segment_cmd + "-%d.ts"
            result = os.system(ffmpeg_segment_cmd)
            if 0 != result:
                self.logger.error("Cannot segment file %s to MPEG-TS. return code = %d" %
                        (tmp_file, result))
                return -3

            #generate m3u8 for MPEG-TS segments
            m3u8_cmd = "m3u8 %s 0 %s.m3u8 10 http://fy2.richitec.com/feiying/" % (tmp_file,
                    tmp_dir+prefix)
            result = os.system(m3u8_cmd)
            if 0 != result:
                self.logger.error("Cannot generate m3u8 for %s segments. return code = %d" %
                        (tmp_file, result))
                return -4

            #save MPEG-TS segments and m3u8 to MogileFS
            wl = os.walk(tmp_dir)
            for w in wl:
                break

            fl = w[2]
            for f in fl:
                result = self._save_mogilefs(self.opts.trackers, domain, f, tmp_dir+f)
                if 0 != result:
                    self.logger.error("Cannot save file %s to MogileFS. return code = %d" %
                            (tmp_dir+f, result))
                    break

            if 0 == result:
                os.rmdir(tmp_dir)
         
        # tmp video file downloaded, and upload it to mogilefs
        result = self._save_mogilefs(self.opts.trackers, domain, fid, tmp_file)
        if 0 != result:
            self.logger.error("Cannot save file %s to MogileFS. return code = %d" % (tmp_file,
                result))

        return 0

    def _save_mogilefs(self, trackers, domain, key, file_path):
        mogupload_cmd = "mogupload --trackers=%s --domain=%s --key='%s' --file='%s'" % (trackers,
                domain, key, file_path)
        r = os.system(mogupload_cmd)
        if 0 == r:
            os.remove(file_path)
        return r

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

    def _download_episode(self, source_id, index, url, domain):
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
        if r == 0:
            self.logger.info('download %s result=%d', data['source_id'], r)
#            req = self.gmclient.submit_job('fy_sphinx_index', j.data, wait_until_complete=False,
#                background=True)
#            self.gmclient.wait_until_jobs_accepted([req])
        else:
            self.logger.error('download %s result=%d', data['source_id'], r)

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

        if r > 0:
            self._update_status(source_id, 4)
        elif r == 0: #
            self._update_status(source_id, 100) #download complete successfully
        else: # r<=0
            self._update_status(source_id, 0) #download nothing

        return r
            

class UpdatingSeriesWorker(BaseWorker):
    name = 'fy_updating_series_download'
    def do_work(self, w, j):
        data = json.loads(j.data)
        source_id = data['source_id']

        self.logger.info('download updating series %s', source_id)
    
        self._update_status(source_id, 102) # 102 begin download new episodes
        r = self._download_multi(data, self.opts.video_domain)

        status = 100
        if r > 0:
             status = 104            
        self._update_status(source_id, status) #download complete successfully
        return r
           

def main():
    parser = OptionParser()
    parser.add_option('-g', '--gearman-servers', dest='gs', default='gearman-server-1:4730,gearman-server-2:4730',
            help='gearman server list')
    parser.add_option('-w', '--worker-type', dest='worker', 
            help='worker type: (movie|series|useries|video)')
    parser.add_option('-m', '--mogilefs-trackers', dest='trackers', default='mogile-tracker-1:7001,mogile-tracker-2:7001', 
            help='mogilefs trackers ---- ip:port[,ip:port]')
    parser.add_option('--mog-video-domain', dest='video_domain', default='fydomain',
            help='mogilefs domain for video files')
    parser.add_option('--mog-image-domain', dest='image_domain', default='fydomain',
            help='mogilefs domain for image files')
    
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

