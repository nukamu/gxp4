#! /usr/bin/env python

from __future__ import with_statement

import sys
import os
sys.path.append(os.pardir)




if __name__ == '__main__':
    if len(sys.argv) < 3:
        sys.exit("Usage: %s [meta_addr] [files]" % (sys.argv[0]))
