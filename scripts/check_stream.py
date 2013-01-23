#! /usr/bin/env python

import sys
import csv
import os.path

if len(sys.argv) != 4:
    sys.exit("Usage: %s [data_path] [meta_dir] [state_dir]")

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
             'hongo205': '133.11.117.29'}

ap_log_file = sys.argv[1]
meta_dir = sys.argv[2]
work_path = os.path.join(sys.argv[3], 'work.txt')

mogami_mnt_path = '/data/local2/mikity/montage/'

ap_dict = {}  # cmd: files (list)
job_id = 0

def get_owner(mogami_path):
    target_path = os.path.join(meta_dir, "montage", mogami_path)
    f = open(target_path, 'r')
    buf = f.read()
    ip = buf.rsplit(",")[0]
    f.close()
    return ip


# parse ap log file
with open(ap_log_file, 'r') as ap_data_f:
    cmd_str = ""
    while True:
        buf = ap_data_f.readline()
        if buf == "":
            break
        if buf[:1] != "[":
            cmd_str = buf.rstrip("\n")
            job_id += 1
            continue
        ap = buf.rstrip("\n").split("\t")
        created = int(ap[3])
        read_log = eval(ap[4])
        write_log = eval(ap[5])
        read_size = 0
        write_size = 0
        for read_data in read_log:
            read_size += read_data[1]
        for write_data in write_log:
            write_size += write_data[1]
        path = ap[2].replace(mogami_mnt_path, '')
        if path not in ap_dict:
            ap_dict[path] = []
        ap_dict[path].append((job_id, read_size, write_size, created, cmd_str))

file_created_location = {}
work_location_dict = {}

work_data = csv.reader(open(work_path, 'r'), delimiter='\t')
work_data.next()
for work in work_data:
    cmd = work[1].replace("  ", " ").replace(" || echo err", "")
    node = host_dict[work[5].split('-')[0]]
    work_location_dict[cmd] = node

all_read_size = 0
all_write_size = 0

org_read_size = 0
int_read_size = 0
out_read_size = 0

org_local_size = 0
int_local_size = 0
out_local_size = 0

int_write_size = 0
out_write_size = 0
final_write_size = 0

#print ap_dict
for path, ap_list in ap_dict.iteritems():
    created_job_id = -1
    for ap in ap_list:
        node = work_location_dict[ap[4]]
        all_read_size += ap[1]
        all_write_size += ap[2]
        if ap[3] == 1:
            assert created_job_id == -1
            file_created_location[path] = node
            created_job_id = ap[0]
    try:
        from_node = get_owner(path)
    except IOError:
        from_node = file_created_location[path]

    for ap in ap_list:
        node = work_location_dict[ap[4]]
        if created_job_id == -1:
            org_read_size += ap[1]
            if node == from_node:
                org_local_size += ap[1]
        elif created_job_id == ap[0]:
            int_read_size += ap[1]
            if node == from_node:
                int_local_size += ap[1]
        else:
            out_read_size += ap[1]
            if node == from_node:
                out_local_size += ap[1]
        

print "============= org ==============="
print "%f%% (%d/%d)" % (100 * org_read_size/float(all_read_size),
                        org_read_size, all_read_size)
print "============= int ==============="
print "%f%% (%d/%d)" % (100 * int_read_size/float(all_read_size),
                        int_read_size, all_read_size)
print "============= out ==============="
print "%f%% (%d/%d)" % (100 * out_read_size/float(all_read_size),
                        out_read_size, all_read_size)

print "============= org local ==============="
print "%f%% (%d/%d)" % (100 * org_local_size/float(all_read_size),
                        org_local_size, all_read_size)
print "============= int local ==============="
print "%f%% (%d/%d)" % (100 * int_local_size/float(all_read_size),
                        int_local_size, all_read_size)
print "============= out local ==============="
print "%f%% (%d/%d)" % (100 * out_local_size/float(all_read_size),
                        out_local_size, all_read_size)

