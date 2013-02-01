#! /usr/bin/env python

# This script was built for plotting results of montage.
# exp_ids are as follows:
#    appid_usual_cent[[1-5]]
#    appid_optimized_cent[[1-5]]
#    appid_usual_dist[[1-5]]
#    appid_optimized_dist[[1-5]]

import os.path
import subprocess
import conf   # applications conf

data_types = ['cent', 'dist']
exp_types = ['usual', 'optimized']

## results
local_d = {}  # ('dist', 'optimized', 'mProjectPP') -> ratio
exec_time_d = {}  # ('dist', 'optimized', 'mProjectPP') -> time(sum)
makespan_d = {}  # ('dist', 'optimized') -> time(makespan)

def get_avg_from_list(l):
    num = len(l)
    sum_value = 0.0
    for value in l:
        sum_value += value
    return sum_value / float(num)

## get result
for data_type in data_types:
    for exp_type in exp_types:
        for i in range(1, 6):
            exp_id = '%s_%s_%s_%d' % (conf.exp_id_prefix, exp_type, data_type, i)
            p = subprocess.Popen(['./show_result.py', exp_id], stdout=subprocess.PIPE)
            p.wait()
            switch_flag = 0
            while True:
                out_str = p.stdout.readline()
                if len(out_str) == 0:
                    break
                out_str = out_str.rstrip('\n')
                if out_str[0] == '=':
                    switch_flag += 1
                    assert switch_flag < 3, switch_flag
                    continue
                if switch_flag == 0:
                    app = out_str.split(',')[0]
                    local_ratio = out_str.split(',')[3]
                    local_ratio = float(local_ratio)
                    if (data_type, exp_type, app) not in local_d:
                        local_d[(data_type, exp_type, app)] = []
                    local_d[(data_type, exp_type, app)].append(local_ratio)
                elif switch_flag == 1:
                    app = out_str.split(',')[0]
                    exec_time = out_str.split(',')[1]
                    exec_time = float(exec_time)
                    if (data_type, exp_type, app) not in exec_time_d:
                        exec_time_d[(data_type, exp_type, app)] = []
                    exec_time_d[(data_type, exp_type, app)].append(exec_time)
                else:
                    makespan = float(out_str)
                    if (data_type, exp_type) not in makespan_d:
                        makespan_d[(data_type, exp_type)] = []
                    makespan_d[(data_type, exp_type)].append(makespan)
        
        for app in conf.apps:
            key = (data_type, exp_type, app)
            local_d[key] = get_avg_from_list(local_d[key])
            exec_time_d[key] = get_avg_from_list(exec_time_d[key])
        key = (data_type, exp_type)
        makespan_d[key] = get_avg_from_list(makespan_d[key])


## output results
for data_type in data_types:
    local_f = open(os.path.join('graph', 'local_' + data_type + '.dat'), 'w+')
    exec_time_f = open(os.path.join('graph', 'sum_' + data_type + '.dat'), 'w+')
    makespan_f = open(os.path.join('graph', 'makespan_' + data_type + '.dat'), 'w+')

    count = 0
    for app in conf.apps:
        local_f.write('%d %f %f\n', (count * conf.graph_gap + 1,
                                   local_d[(data_type, 'usual', app)],
                                     local_d[(data_type, 'optimized', app)]))
        count += 1
    local_f.close()

    count = 0
    for app in conf.apps:
        exec_time_f.write('%d %f %f\n', (count * conf.graph_gap + 1,
                                   exec_time_d[(data_type, 'usual', app)],
                                     exec_time_d[(data_type, 'optimized', app)]))
        count += 1
    exec_time_f.close()

    makespan_f.write('1 %f\n' % (makespan_d[(data_type, 'usual')]))
    makespan_f.write('2 %f\n'% (makespan_d[(data_type, 'optimized')]))
    makespan_f.close()
