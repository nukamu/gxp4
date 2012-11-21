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
            
        except Exception:
            shutil.rmtree(self.working_dir)
            raise

    def tearDown(self):
        ## remove the working directory
        shutil.rmtree(self.working_dir)

    def test_access(self):
        pass

    def test_listdir(self):
        pass

if __name__ == '__main__':
    unittest.main()
