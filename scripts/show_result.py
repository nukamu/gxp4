#! /usr/bin/env python
# -*- coding: utf-8 -*-


import sys,os.path

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
    def __init__(self, f_data_path, f_result_path, meta_dir):
        self.all_job_sum_time = 0.0
        self.meta_dir = meta_dir

        self.access_dict = {}  # {cmd: {file_name: access}}
        # このコマンドがこのファイルにこの量アクセスを全部知っている
        f_data = open(f_data_path, 'r')
        while True:
            line = f_data.readline()
            if line == '':
                break
            l = line.rsplit("|")
            cmd = " ".join(eval(l[0]))
            # mogami 上のpathにする
            file_path = l[2].replace("/data/local2/mikity", '')
            read_log = eval(l[4])
            read_size = 0
            for log in read_log:
                read_size += log[1]
            
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
            self.all_job_sum_time += float(l[13])

            if args[0] not in self.job_execution_time:
                self.job_execution_time[args[0]] = 0.0
                self.job_execution_count[args[0]] = 0
            self.job_execution_time[args[0]] += float(l[10])
            self.job_execution_count[args[0]] += 1

            ip = host_dict[l[7].rsplit('-')[0]]
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

        for app, d in result_dict.iteritems():
            print "%s,%d,%d,%f" % (app, d['local'], d['remote'], d['local'] * 100/float(d['local'] + d['remote']))
        print "all,%d,%d,%f" % (sum_local, sum_remote, sum_local * 100/float(sum_local + sum_remote))
        print "================="

        all_exec_time = 0.0
        for app, exec_time in self.job_execution_time.iteritems():
            count = self.job_execution_count[app]
            average_time = exec_time / float(count)
            print "%s,%f,%d" % (app, exec_time, count)
            all_exec_time += exec_time
        print "all,%f" % (all_exec_time)
        print "================="


if __name__ == '__main__':
    if len(sys.argv) != 4:
        sys.exit("Usage: %s [data_path] [result_path] [meta_dir]")
    e = Exp_Analyzer(sys.argv[-3], sys.argv[-2], sys.argv[-1])
    e.analyze()
