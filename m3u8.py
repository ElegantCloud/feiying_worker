# m3u8.py

import sys
import os
from optparse import OptionParser
from ctypes import *
from ffmpeg import *


def extract_filename(str):
    basename = os.path.basename(str)
    str_list = basename.split('.')
    length = len(str_list)
    if length != 2:
        print "Error file name format %s" % str
        return None
    else:
        return str_list[0]

def segment(input_file, output_dir, segment_time, http_prefix):
    file_name = extract_filename(input_file)
    if file_name == None:
        return -1

    if not output_dir.endswith('/'):
        output_dir = output_dir + '/'

    ts_file_pattern = file_name + "-high-%d.ts"

    #devide video file to MPEGTS segments.
    ffmpeg_segment_cmd = "ffmpeg -i %s -f segment -segment_time %d " \
        "-segment_format mpegts -codec copy -bsf:v h264_mp4toannexb " \
        "-map 0 %s" % (input_file, segment_time, output_dir+ts_file_pattern)

    print "Execute command: %s" % ffmpeg_segment_cmd
    r = os.system(ffmpeg_segment_cmd)
    if 0 != r:
        print "Cannot segment the input file %s to MPEGTS." % input_file
        return -2;

    #extract AAC audio from MPEGTS segments
    i = 0
    aac_file_pattern = file_name + "-audio-%d.aac"
    while True:
        ts_file_name = output_dir + ts_file_pattern % i
        if not os.path.exists(ts_file_name):
            print "No more MPEGTS segment files."
            break

        aac_file_name = output_dir + aac_file_pattern % i
        ffmpeg_extract_audio_cmd = "ffmpeg -i %s -vn -acodec copy %s" \
                % (ts_file_name, aac_file_name)
        
        r = os.system(ffmpeg_extract_audio_cmd)
        if 0 != r :
            print "Cannot extract audio stream from %" % ts_file_name
            return -3

        i = i+1
        
    #generate m3u8 index file for MPEGTS segments
    m3u8_file_name = output_dir + file_name + "-high.m3u8"
    m3u8_cmd = "m3u8 %s %s %d %s %d %s " % (output_dir, ts_file_pattern, 0,
            m3u8_file_name, segment_time, http_prefix )
    r = os.system(m3u8_cmd)
    if 0 != r:
        print "Cannot generate %s" % m3u8_file_name
        return -4

    #generate m3u8 index file for audio segments
    m3u8_file_name = output_dir + file_name + "-audio.m3u8"
    m3u8_cmd = "m3u8 %s %s %d %s %d %s " % (output_dir, aac_file_pattern, 0,
            m3u8_file_name, segment_time, http_prefix )
    r = os.system(m3u8_cmd)
    if 0 != r:
        print "Cannot generate %s" % m3u8_file_name
        return -5

    #generate playlist 
    m3u8_file_name = output_dir + file_name + ".m3u8"
    

    return 0

def main():
    parser = OptionParser()
    parser.add_option('--input_file', dest='input_file')
    parser.add_option('--output_dir', dest='output_dir')
    parser.add_option('--segment_time', dest='segment_time', default=10,
            type='int')
    parser.add_option('--http_prefix', dest='http_prefix')
    (options, args) = parser.parse_args()
    
    if options.input_file==None or options.output_dir==None or \
        options.http_prefix==None:
        parser.print_help()
        sys.exit()

    r = segment(options.input_file, options.output_dir, options.segment_time,\
        options.http_prefix)

    return r


if __name__ =='__main__':
    main()
