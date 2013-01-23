#! /usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import sys,os.path

apps_list = ['mProjectPP',
        'mDiffFit',
        'mConcatFit',
        'mBackground',
        'mImgtbl',
        'mAdd',
        'mShrink',
        'mBgModel',
        'mJPEG',
        ]

host_dict = {'tsukuba100': '163.220.103.90',
             'tsukuba101': '163.220.103.91',
             'tsukuba102': '163.220.103.92',
             'tsukuba103': '163.220.103.93',
             'tsukuba104': '163.220.103.94',
             'tsukuba105': '163.220.103.95',
             'hongo200': '133.11.117.24',
             'hongo201': '133.11.117.25',
             'hongo202': '133.11.117.26',
             'hongo203': '133.11.117.27',
             'hongo204': '133.11.117.28',
             'hongo205': '133.11.117.29',
             'hongo206': '133.11.117.30',
             'hongo207': '133.11.117.31',
             'hongo208': '133.11.117.32',
             'hongo400': '133.11.117.36',
             'hongo401': '133.11.117.37',
             'hongo402': '133.11.117.38',
             'hongo403': '133.11.117.39',
             'hongo404': '133.11.117.40',
             'hongo405': '133.11.117.41',
             'hongo406': '133.11.117.42',
             'huscs000': '133.50.19.5',
             'huscs001': '133.50.19.6',
             'huscs002': '133.50.19.7',
             'huscs003': '133.50.19.8',
             'huscs004': '133.50.19.9',
             'huscs005': '133.50.19.10',
             'huscs006': '133.50.19.11',
             'huscs007': '133.50.19.12',
             'huscs008': '133.50.19.13',
             'huscs009': '133.50.19.14',
             'huscs010': '133.50.19.15',
             'huscs011': '133.50.19.16',
             'huscs012': '133.50.19.17',
             'huscs013': '133.50.19.18',
             'huscs014': '133.50.19.19',
             'huscs015': '133.50.19.20',
             'huscs016': '133.50.19.21',
             'huscs017': '133.50.19.22',
             'huscs018': '133.50.19.23',
             'huscs019': '133.50.19.24',
             'kyoto100': '192.58.9.32',
             'kyoto101': '192.58.9.33',
             'kyoto102': '192.58.9.34',
             'kyoto103': '192.58.9.35',
             'kyoto104': '192.58.9.36',
             'kyoto105': '192.58.9.37',
             'kyoto106': '192.58.9.38',
}




MOGAMI_MOUNT = "/data/local2/mikity/mnt"

class Exp_Analyzer():
    def __init__(self, f_aplog_path, f_result_path):
        self.all_job_sum_time = 0.0
        self.sum_local = 0
        self.sum_remote = 0

        self.result_dict = {}
        local_size = 0
        remote_size = 0

        ap_data = csv.reader(open(f_aplog_path), delimiter='\t')

        for ap in ap_data:
            cmd = ap[0]
            app = cmd.split(' ')[0]
            filename = ap[1]
            local = eval(ap[2])
            read = int(ap[3])
            write = int(ap[4])
            if app not in self.result_dict:
                self.result_dict[app] = {}
                self.result_dict[app]['local'] = 0
                self.result_dict[app]['remote'] = 0

            if local == True:
                self.result_dict[app]['local'] += read
                self.sum_local += read
            else:
                self.result_dict[app]['remote'] += read
                self.sum_remote += read

        self.job_execution_time = {}
        self.job_execution_count = {}
        # コマンドがどこにディスパッチされたか知っている
        f_result = open(f_result_path, 'r')
        line = f_result.readline()
        while True:
            line = f_result.readline()
            if line == '':
                break
            l = line.rsplit('\t')
            cmd = l[1].replace("  ", " ")
            cmd = cmd.replace(" || echo err", "")
            args = cmd.split(" ")
            self.all_job_sum_time += float(l[13])

            if args[0] not in self.job_execution_time:
                self.job_execution_time[args[0]] = 0.0
                self.job_execution_count[args[0]] = 0
            self.job_execution_time[args[0]] += float(l[10])
            self.job_execution_count[args[0]] += 1
        f_result.close()

    def analyze(self, ):
        """結果として得たいもの
        = mProjectPPは3/100ファイルをローカルから，100/1000byteをローカルから
        """
        self.result_dict['mDiffFit']['local'] += self.result_dict['mFitplane']['local']
        self.result_dict['mDiffFit']['remote'] += self.result_dict['mFitplane']['remote']
        self.result_dict['mDiffFit']['local'] += self.result_dict['mDiff']['local']
        self.result_dict['mDiffFit']['remote'] += self.result_dict['mDiff']['remote']

        del self.result_dict['mFitplane']
        del self.result_dict['mDiff']


        for app in apps_list:
            d = self.result_dict[app]
            print "%s,%d,%d,%f" % (app, d['local'], d['remote'],
                                   d['local'] * 100/float(d['local'] + d['remote']))
        print "all,%d,%d,%f" % (self.sum_local, self.sum_remote,
                                self.sum_local * 100/float(self.sum_local + self.sum_remote))
        print "================="

        all_exec_time = 0.0
        for app in apps_list:
            exec_time = self.job_execution_time[app]
            count = self.job_execution_count[app]
            average_time = exec_time / float(count)
            print "%s,%f,%d" % (app, exec_time, count)
            all_exec_time += exec_time
        print "all,%f" % (all_exec_time)
        print "================="


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.exit("Usage: %s [aplog_path] [result_path]")
    e = Exp_Analyzer(sys.argv[1], sys.argv[2])
    e.analyze()
