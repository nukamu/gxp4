#! /usr/bin/env python

import sys
import csv

if len(sys.argv) != 2:
    sys.exit("Usage: %s [work]" % (sys.argv[0]))

records = csv.reader(open(sys.argv[1], 'r'), delimiter='\t')

records.next()

for record in records:
    cmds = record[1].rsplit(' ')
    if cmds[0] == 'echo':
        continue
    elif cmds[0] == 'if' and cmds[3][-3:] == 'md5':
        continue
    print record[1]
