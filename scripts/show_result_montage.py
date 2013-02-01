#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys,os.path
import conf

apps_list = conf.apps
host_dict = conf.host_dict
MOGAMI_MOUNT = "/data/local2/mikity/mnt"

class Exp_Analyzer():
    def __init__(self, f_data_path, f_result_path, meta_dir):
        self.meta_dir = meta_dir

        self.access_dict = {}  # {cmd: {file_name: access}}
        # このコマンドがこのファイルにこの量アクセスを全部知っている
        f_data = open(f_data_path, 'r')
        cmd = ''
        while True:
            line = f_data.readline()
            if line == '':
                break
            if line[0] != '/':
                cmd = line.rstrip('\n')
                continue
            l = line.rsplit("\t")
            file_path = l[0].replace("/data/local2/mikity", '')
            local = eval(l[1])
            read_size = int(l[2])
            write_size = int(l[3])
            
            # 結果を代入
            if cmd not in self.access_dict:
                self.access_dict[cmd] = {}
            if file_path not in self.access_dict[cmd]:
                self.access_dict[cmd][file_path] = 0
            self.access_dict[cmd][file_path] += read_size
        f_data.close()

        self.dispatch_dict = {}
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

            if f_result_path.find('usual') != -1:
                job_execution_time = float(l[10])
                man = l[7].rsplit('-')[0]
            else:
                job_execution_time = float(l[4])
                man = l[5].rsplit('-')[0]

            if args[0] not in self.job_execution_time:
                self.job_execution_time[args[0]] = 0.0
                self.job_execution_count[args[0]] = 0
            self.job_execution_time[args[0]] += job_execution_time
            self.job_execution_count[args[0]] += 1

            ip = host_dict[man]
            if args[0] == "mDiffFit":
                tmp_cmd = "mFitplane -b 0 %s" % (args[5])
                self.dispatch_dict[tmp_cmd] = ip
                tmp_cmd = "mDiff %s %s %s %s" % (args[3], args[4],
                                                 args[5], args[6])
                self.dispatch_dict[tmp_cmd] = ip
            self.dispatch_dict[cmd] = ip
        f_result.close()

    def get_owner(self, mogami_path):
        f = open(os.path.join(self.meta_dir + mogami_path), 'r')
        buf = f.read()
        ip = buf.rsplit(",")[0]
        f.close()
        return ip

    def analyze(self, ):
        """結果として得たいもの
        = mProjectPPは3/100ファイルをローカルから，100/1000byteをローカルから
        """
        sum_local = 0
        sum_remote = 0

        result_dict = {}
        for cmd, file_dict in self.access_dict.iteritems():
            dispatched = self.dispatch_dict[cmd]
            local_size = 0
            remote_size = 0
            for file_path, size in file_dict.iteritems():
                place = self.get_owner(file_path)
                if place == dispatched:
                    local_size += size
                else:
                    remote_size += size
            app = cmd.rsplit(" ")[0]
            if app not in result_dict:
                result_dict[app] = {}
                result_dict[app]['local'] = 0
                result_dict[app]['remote'] = 0
            result_dict[app]['local'] += local_size
            result_dict[app]['remote'] += remote_size
            sum_remote += remote_size
            sum_local += local_size

        result_dict['mDiffFit']['local'] += result_dict['mFitplane']['local']
        result_dict['mDiffFit']['remote'] += result_dict['mFitplane']['remote']
        result_dict['mDiffFit']['local'] += result_dict['mDiff']['local']
        result_dict['mDiffFit']['remote'] += result_dict['mDiff']['remote']

        del result_dict['mFitplane']
        del result_dict['mDiff']


        for app in apps_list:
            d = result_dict[app]

            print "%s,%d,%d,%f" % (app, d['local'], d['remote'], d['local'] * 100/float(d['local'] + d['remote']))
        print "Al,%d,%d,%f" % (sum_local, sum_remote, sum_local * 100/float(sum_local + sum_remote))
        print "================="

        all_exec_time = 0.0
        for app in apps_list:
            exec_time = self.job_execution_time[app]
            count = self.job_execution_count[app]
            average_time = exec_time / float(count)
            print "%s,%f,%d" % (app, exec_time, count)
            all_exec_time += exec_time
        print "All,%f" % (all_exec_time)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit("Usage: %s [exp_id]")
    exp_id = sys.argv[1].replace(conf.exp_id_prefix + '_', '')
    (exp_type, data_type, num) = exp_id.split('_')
    num = int(num)
    if data_type == 'cent':
        sys.argv[1] = sys.argv[1].replace('_cent', '')
    if exp_id.find('usual') != -1:
        e = Exp_Analyzer('data/data2.dat',
                         os.path.join('state', sys.argv[1], 'work.txt'),
                         os.path.join('meta', sys.argv[1]))
    e.analyze()
