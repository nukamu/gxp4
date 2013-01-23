#! /usr/bin/env python

import sys
sys.path.append("/home/miki/git/gxp4")

import mogami_scheduler
import csv
import cPickle


if len(sys.argv) != 3:
    sys.exit("Usage: %s [feature_file] [ap_log_data]" % (sys.argv[0]))

feature_file = sys.argv[1]
ap_log_file = sys.argv[2]

with open(feature_file, 'r') as f:
    (feature_dict, pid_dict) = cPickle.loads(f.read())

ap_dict = {}  # cmd: files (list)
cmd_list = []
app_dict = {}

# parse ap log file
with open(ap_log_file, 'r') as ap_data_f:
    cmd_str = ""
    while True:
        buf = ap_data_f.readline()
        if buf == "":
            break
        if buf[:1] != "[":
            cmd_str = buf.rstrip("\n")
            app = cmd_str.split(' ')[0]
            if app == 'mDiff' or app == 'mFitplane':
                app = 'mDiffFit'
            if app not in app_dict:
                app_dict[app] = []
            app_dict[app].append(cmd_str)
            continue
        ap = buf.rstrip("\n").split("\t")
        read_log = eval(ap[4])
        read_size = 0
        for read_data in read_log:
            read_size += read_data[1]
        if read_size == 0:
            continue
        path = ap[2]
        if cmd_str not in ap_dict:
            ap_dict[cmd_str] = []
        ap_dict[cmd_str].append(path)


for k, v in app_dict.iteritems():
    sum_all_recall_cnt = 0
    sum_fit_recall_cnt = 0
    sum_all_precise_cnt = 0
    sum_fit_precise_cnt = 0
    cnt_pm = 0

    job_num = len(v)

    for cmd in v:
        file_dict = mogami_scheduler.file_from_feature(cmd.split(" "), feature_dict, pid_dict, 
                                                       "/data/local2/mikity/mnt/montage/solvers/gxp_make")
        predict_files = list(set(file_dict.keys()))
        access_files = list(set(ap_dict[cmd]))

        #print predict_files, len(predict_files)
        #print access_files, len(access_files)
        #sys.exit(0)
    
        all_recall_cnt = 0
        fit_recall_cnt = 0
        for f in access_files:
            f = f.replace("/home/miki/svn/workflows/apps", '')
            f = f.replace("/data/local2/mikity", '')
            #f = '/data/local2/mikity/' + f
            sum_all_recall_cnt += 1
            all_recall_cnt += 1
            if f in predict_files:
                sum_fit_recall_cnt += 1
                fit_recall_cnt += 1
            elif f[-3:] == '.pm':
                sum_fit_recall_cnt += 1
                fit_recall_cnt += 1
                cnt_pm += 1
            elif f.find('module') != -1:
                sum_fit_recall_cnt += 1
                fit_recall_cnt += 1
                cnt_pm += 1
            else:
                #print "** [not found recall] %s **" % (f)
                pass

        all_precise_cnt = 0
        fit_precise_cnt = 0
        for f in predict_files:
            f = f.replace("/home/miki/svn/workflows/apps", '')
            f = f.replace("/data/local2/mikity", '')
            f = '/data/local2/mikity' + f
            sum_all_precise_cnt += 1
            all_precise_cnt += 1
            if f in access_files:
                sum_fit_precise_cnt += 1
                fit_precise_cnt += 1
            else:
                #print "** [not found precision] %s **" % (f)            
                pass

    print "=============recall (%s: %d)===============" % (k, job_num)
    print "%f%% (%d/%d)" % (100 * sum_fit_recall_cnt/float(sum_all_recall_cnt),
                            sum_fit_recall_cnt, sum_all_recall_cnt)
    
    print "=============precision (%s)===============" % (k)
    print "%f%% (%d/%d)" % (100 * sum_fit_precise_cnt/float(sum_all_precise_cnt),
                            sum_fit_precise_cnt, sum_all_precise_cnt)

#print "=============recall (size)==============="
#print "%f%% (%d/%d)" % (100 * sum_fit_recall_size/float(sum_all_recall_size),
#                        sum_fit_recall_size, sum_all_recall_size)

#print "=============precision (size)==============="
#print "%f%% (%d/%d)" % (100 * sum_fit_precise_size/float(sum_all_precise_size),
#                        sum_fit_precise_size, sum_all_precise_size)


    #print "%s\t%f\t%d\t%d\t%f\t%d\t%d" % (cmd, fit_recall_cnt/float(all_recall_cnt),
    #                      fit_recall_cnt, all_recall_cnt, 
    #                      fit_precise_cnt/float(all_precise_cnt),
    #                      fit_precise_cnt, all_precise_cnt)
    
