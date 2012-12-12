#! /usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import sys
import os
sys.path.append(os.pardir)
import metadata

import os.path
import errno
import tempfile
import shutil

class TestMetadataMngComponent(unittest.TestCase):
    """In these test, the results of usual os method 
    and metadata management methods (fs and db) are compared.
    """
    def setUp(self):
        ## create a working directory
        self.working_dir = tempfile.mkdtemp()

        try:
            os.mkdir(os.path.join(self.working_dir, 'fs'))
            self.meta_fs = metadata.MogamiMetaFS(os.path.join(
                    self.working_dir, 'fs'))
            self.meta_db = metadata.MogamiMetaDB(self.working_dir)

            ## create some files and directories for test
            self.file_list = ['/a/d', '/a/b/e', '/a/b/f/h',
                              '/a/b/f/i', '/a/b/g/n',
                              '/a/c/j/l', '/a/c/j/m']
            self.dir_list = ['/a', '/a/b', '/a/c',
                                   '/a/b/f', '/a/b/g',
                                   '/a/c/j', '/a/c/k']

            for meta_rep in [self.meta_fs, self.meta_db]:
                for dir_name in self.dir_list:
                    meta_rep.mkdir(dir_name)
                for file_name in self.file_list:
                    meta_rep.create(file_name, 0, [], 'dest', 'destpath')
            
        except Exception:
            shutil.rmtree(self.working_dir)
            raise

    def tearDown(self):
        ## remove the working directory
        shutil.rmtree(self.working_dir)

    def test_getattr(self):
        pass

    def test_readdir(self):
        fs_result = self.meta_fs.readdir('/a')
        db_result = self.meta_db.readdir('/a')
        for ele in fs_result:
            self.assertNotEqual(db_result.count(ele), 0)

if __name__ == '__main__':
    unittest.main()
