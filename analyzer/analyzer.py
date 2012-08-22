#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import os.path
import sqlite3
import cPickle


class MogamiWorkflowIOAnalyzer(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self.db_conn = sqlite3.connect(self.db_path)
        self.db_cur = self.db_conn.cursor()

        self.command_dict = {}
        self.exe_cwd = None

    def parse_data_from_db(self, ):
        # select data
        self.db_cur.execute("""
        SELECT * FROM ap_log;
        """)

        count = 0
        ret_list = []
        for data in self.db_cur:
            job_id = int(data[0])
            cmd = eval(data[1])
            pid = int(data[2])
            path = data[3]
            created = int(data[4])
            read_log = eval(data[5])
            write_log = eval(data[6])
            ret_list.append((job_id, cmd, pid, path, created,
                             read_log, write_log))

        # select data
        self.db_cur.execute("""
        SELECT cwd FROM workflow_env;
        """)
        self.exe_cwd = self.db_cur.fetchone()[0]
        print "cwd = %s" % (self.exe_cwd)

        # close the connection to db
        self.db_conn.close()
        self.db_cur = None

        return ret_list
        
    def analize(self, ):
        self.make_sets()

    def make_sets(self, ):
        """
        This function should be called in self.analize().
        """
        self.ap_dict = {}   # jobid: access_file_(feature_)list
        self.arg_job_dict = {}  # ("argv[0]", "argv[k]"): jobid

        aplog_list = self.parse_data_from_db()
                
        for ap in aplog_list:
            job_id = ap[0]
            cmd = ap[1]
            pid = ap[2]
            path = ap[3]
            created = ap[4]
            read_log = ap[5]
            write_log = ap[6]

            if job_id not in self.ap_dict:
                # insert job_id in arg_job_dict
                app = cmd[0]
                for arg in cmd:
                    arg_set = (app, arg)
                    if arg_set not in self.arg_job_dict:
                        self.arg_job_dict[arg_set] = []
                    self.arg_job_dict[arg_set].append(job_id)
                # create list for aplog
                self.ap_dict[job_id] = []
            
            (read_feature, write_feature,
             read_size, write_size) = self.cmd_file_feature(
                cmd, path, read_log, write_log)
            if read_feature is not None:
                self.ap_dict[job_id].append((read_feature, read_size))

    def cmd_file_feature(self, cmd, path, read_log, write_log):
        assert self.exe_cwd != None

        counter = 0
        former_arg = ""
        
        read_feature = None
        read_size = self.size_from_iolog(read_log)
        write_feature = None
        write_size = self.size_from_iolog(write_log)
        
        for arg in cmd:
            cmd_file = os.path.normpath(os.path.join(self.exe_cwd, arg))
            likely = self.my_str_find(cmd_file, path)

            if cmd_file == path:
                option = ""
                if former_arg[:1] == '-':
                    option = former_arg
                if read_size is not  0:
                    read_feature = (-1, -1, '', option, counter)
                if write_size is not 0:
                    write_feature = (-1, -1, '', option, counter)
                break
            elif likely != None:
                option = ""
                if former_arg[:1] == '-':
                    option = former_arg
                if read_size is not 0:
                    read_feature = (likely[0], likely[1], likely[2],
                                    option, counter)
                if write_size is not 0:
                    write_feature = (likely[0], likely[1], likely[2],
                                     option, counter)

            counter += 1
            former_arg = arg

        if path.find(self.exe_cwd) != -1:
            rel_path = "." + path.replace(self.exe_cwd, '')
        else:
            rel_path = path
        print rel_path
        if read_feature == None:
            if read_size is not 0:
                read_feature = (-1, -1, "", rel_path, -1)
        if write_feature == None:
            if write_size is not 0:
                write_feature = (-1, -1, "", rel_path, -1)
        return (read_feature, write_feature, read_size, write_size)

    def size_from_iolog(self, log_list):
        """
        @param log list like [(0, 1024), (2048, 1024)]
        """
        size = 0
        for log in log_list:
            size += log[1]
        return size

    def my_str_find(self, str1, str2):
        """
        """
        str1_base = os.path.basename(str1)
        str2_base = os.path.basename(str2)

        if len(str1_base) > len(str2_base):
            return None

        counter = 0
        while counter < len(str1_base):
            if str1_base[counter] == str2_base[counter]:
                counter += 1
            else:
                break

        from_left_offset = counter - 1

        counter = 0
        while counter < len(str1_base):
            if str1_base[-counter] == str2_base[-counter]:
                counter += 1
            else:
                break

        from_right_offset = counter - 1

        if from_left_offset == -1 and from_right_offset == -1:
            return None
        plus_str = str2_base[from_left_offset + 1: -from_right_offset]
        if plus_str == "":
            return None
        if (from_left_offset + from_right_offset + 1) != len(str1_base):
            return None

        #print str1_base, str2_base
        return (from_left_offset, from_right_offset, plus_str)

    def output(self, output_file):
        """Output result to the specified file.

        @param output_file path of output file
        """
        f = open(output_file, 'w+')
        f.write(cPickle.dumps((self.ap_dict, self.arg_job_dict)))
        f.close()

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    if len(sys.argv) != 3:
        sys.exit("Usage: %s [db_path] [output_file]" % (sys.argv[0]))
    workflow_analyzer = MogamiWorkflowIOAnalyzer(sys.argv[1])
    workflow_analyzer.analize()
    workflow_analyzer.output(sys.argv[2])
