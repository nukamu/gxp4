#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import csv
import sqlite3
import os.path
import re

class CreateDB():
    def __init__(self, db_path, ap_file, state_dir):
        self.db_path = db_path
        self.ap_file = ap_file
        self.state_dir = state_dir

    def main(self, ):
        ret = self.create_db()
        if ret != 0:
            sys.exit("failed in creating DB (name: %s)" % (self.db_path))
        self.create_session()
        try:
            self.insert_data()
        except Exception, e:
            os.remove(self.db_path)
            raise

    def create_db(self, ):
        if os.path.exists(self.db_path):
            os.rename(self.db_path, self.db_path + '.org')
        scripts_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        print sys.argv
        return os.system("sqlite3 %s < %s" %
                         (self.db_path, os.path.join(scripts_dir,
                                                     'create_tables.sql')))

    def create_session(self, ):
        self.db_conn = sqlite3.connect(self.db_path)
        self.db_cur = self.db_conn.cursor()

    def insert_data(self, ):
        """
        @return 0 with success, -1 with error
        """
        # insert all ap
        aplogs = csv.reader(open(self.ap_file, 'r'), delimiter='\t')

        cmd_dict = {}  # cmd (tuple) -> job_id
        for ap in aplogs:
            if ap.count("") != 0:
                ap.remove("")
            assert len(ap) == 7, len(ap)
            self.db_cur.execute("""
                INSERT INTO ap_log (
                job_id, cmd, pid, file_path, created, read_data, write_data)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, ap)
        self.db_conn.commit()
        
        cwd = '/data/local2/mikity/mnt/CaseFrameConst/solvers/gxp_make'
        
        #xmp_mount = "/data/local2/mikity/xmp_mnt"
        #cwd_re = re.compile('<tr><td>cwd</td><td>(?P<cwd>[^<]+)</td></tr>')
        # insert env
        #with open(os.path.join(self.state_dir, 'index.html'), 'r') as f:
        #    while True:
        #        line_data = f.readline()
        #        if line_data == "":
        #            break
        #        result = cwd_re.match(line_data)
        #        if result != None:
        #            cwd = result.group('cwd').replace(xmp_mount, '')
        #            break
        #print cwd
        
        self.db_cur.execute("""
        INSERT INTO workflow_env (cwd) VALUES (?)""", (cwd, ))
        self.db_conn.commit()

        return 0

if __name__ == '__main__':
    # args check
    if len(sys.argv) != 4:
        sys.exit("Usage: %s [ap_file] [state_dir] [out_db_name]" % (sys.argv[0]))
    ap_file = sys.argv[1]
    state_dir = sys.argv[2]
    db_name = sys.argv[3]

    db = CreateDB(db_name, ap_file, state_dir)
    db.main()
