#! /usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import sys
import os
sys.path.append(os.pardir)
import tempfile
import shutil

class TestMogamiBasic(unittest.TestCase):
    """In these test, the results of usual os method 
    and metadata management methods (fs and db) are compared.
    """
    def setUp(self):
        ## create a working directory
        self.working_dir = tempfile.mkdtemp()

        try:
            
            self.meta_dir = os.path.join(self.working_dir, 'meta')
            self.data_dir = os.path.join(self.working_dir, 'data')
            self.fs_dir = os.path.join(self.working_dir, 'mnt')

            os.mkdir(self.meta_dir)
            os.mkdir(self.data_dir)
            os.mkdir(self.fs_dir)
            
        except Exception:
            shutil.rmtree(self.working_dir)
            raise

    def tearDown(self):
        ## remove the working directory
        shutil.rmtree(self.working_dir)

    def test_basic1(self):
        pass

    def test_basic2(self):
        pass

if __name__ == '__main__':
    unittest.main()
