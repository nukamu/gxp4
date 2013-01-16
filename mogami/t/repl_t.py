#! /usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import sys
import os
sys.path.append(os.pardir)
import 

import os.path
import errno
import tempfile
import shutil

class TestReplication(unittest.TestCase):
    """
    """
    def setUp(self):
        ## create a working directory
        self.working_dir = tempfile.mkdtemp()

        try:
            os.mkdir(os.path.join(self.working_dir, 'fs'))
            
        except Exception:
            shutil.rmtree(self.working_dir)
            raise

    def tearDown(self):
        ## remove the working directory
        shutil.rmtree(self.working_dir)

    def test_getattr(self):
        pass

    def test_readdir(self):
        pass

if __name__ == '__main__':
    unittest.main()
