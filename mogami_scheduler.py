#!/usr/bin/env python

import socket,cPickle,os.path,cStringIO,time

# TODO: this should be retrieved from system automatically 
MOGAMI_MOUNT = "/data/local2/mikity/mnt"

def file_from_feature(cmd, feature_dict, arg_job_dict, cwd):
    """
    @param cmd list of arguments
    @param feature_dict {job_id: [(feature, size)*]}
    """
    job_id_list = []
    app = cmd[0]
    for arg in cmd:
        try:
            job_id_list.extend(arg_job_dict[(app, arg)])
        except KeyError:
            continue
    job_id_set = list(set(job_id_list))

    # select max jobs' job_id
    max_job_list = []
    max_num = 0
    for job_id in job_id_set:
        num = job_id_list.count(job_id)
        if max_num < num:
            max_num = num
            max_job_list = [job_id]
        elif max_num == num:
            max_job_list.append(job_id)

    # set features to use from now
    read_features = []
    for job_id in max_job_list:
        read_features.append(feature_dict[job_id])

    ret_file_dict = {}
    for features in read_features:
        for (feature, size) in features:
            start = feature[0]
            end = feature[1]
            plus_str = feature[2]
            option = feature[3]
            count = feature[4]
        
            if plus_str == "":
                if count == -1:
                    # itself
                    filename = os.path.join(cwd, option)
                    filename = os.path.normpath(filename.replace(MOGAMI_MOUNT, ""))
                    if filename not in ret_file_dict:
                        ret_file_dict[filename] = size
                    else:
                        ret_file_dict[filename] += size
                
                counter = 0
                for arg in cmd:
                    if counter == count:
                        filename = os.path.join(cwd, arg)
                        filename = os.path.normpath(filename.replace(MOGAMI_MOUNT, ""))
                        if arg not in ret_file_dict:
                            ret_file_dict[filename] = size
                        else:
                            ret_file_dict[filename] += size
                    if option == arg:
                        filename = os.path.join(cwd, cmd[counter + 1])
                        filename = os.path.normpath(filename.replace(MOGAMI_MOUNT, ""))
                        if filename not in ret_file_dict:
                            ret_file_dict[filename] = size
                        else:
                            ret_file_dict[filename] += size
                    counter += 1
            else:
                counter = 0
                for arg in cmd:
                    if counter == count:
                        if len(arg) < start + end:
                            counter += 1
                            continue
                        if end > 0:
                            filename = arg[:start + 1] + plus_str + arg[-end:]
                        else:
                            filename = arg[:start + 1] + plus_str
                        filename = os.path.join(cwd, filename)
                        filename = os.path.normpath(filename.replace(MOGAMI_MOUNT, ""))
                        if filename not in ret_file_dict:
                            ret_file_dict[filename] = size
                        else:
                            ret_file_dict[filename] += size
                    if option == arg:
                        if len(cmd[counter + 1]) < start + end:
                            counter += 1
                            continue
                        filename = cmd[counter + 1][:start + 1] + plus_str + cmd[counter + 1][-end:]
                        filename = os.path.join(cwd, filename)
                        filename = os.path.normpath(filename.replace(MOGAMI_MOUNT, ""))

                        if filename not in ret_file_dict:
                            ret_file_dict[filename] = size
                        else:
                            ret_file_dict[filename] += size
                    counter += 1
    return ret_file_dict


class MogamiChanneltoFileAsk():
    def __init__(self, mds_host):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # decided statically
        self.sock.connect((mds_host, 15806))
        self.bufsize = 1024

    def sendall(self, data):
        """send all data

        @param data data to send
        """
        self.sock.sendall(data)
        return len(data)

    def recvall(self, length):
        """recv all data

        This function may return less data than required.
        (when connection closed)
        @param length length of required data
        """
        buf = cStringIO.StringIO()
        recvlen = 0
        while recvlen < length:
            try:
                recvdata = self.sock.recv(length - recvlen)
            except Exception, e:
                sock.close()
                break
            if recvdata == "":
                sock.close()
                break
            buf.write(recvdata)
            recvlen += len(recvdata)
        data = buf.getvalue()
        return data

    def send_msg(self, data):
        """send some data which is fixed size (conf.bufsize)

        @param data 
        """
        buf = cPickle.dumps(data)
        while self.bufsize - 3 < len(buf):
            ret = self.sendall(buf[:self.bufsize - 3] + "---")
            if ret == None:
                break
            buf = buf[self.bufsize - 3:]
        buf = buf + "-" * (self.bufsize - len(buf) - 3) + "end"
        self.sendall(buf)

    def recv_msg(self, ):
        """
        """
        res_buf = cStringIO.StringIO()
        buf = self.recvall(self.bufsize)
        if len(buf) != self.bufsize:
            return None
        res_buf.write(buf[:-3])
        while buf[-3:] != "end":
            buf = self.recvall(self.bufsize)
            res_buf.write(buf[:-3])
        ret = cPickle.loads(res_buf.getvalue())
        return ret

    def req_fileask(self, file_list):
        """request to ask the location of the files.
        
        @param file_list the list of files to ask the locations
        """
        self.send_msg((30, file_list))
        ans = self.recv_msg()
        return ans

def parse_runs(runs):
    cmd_list = []
    run_dict = {}
    for run in runs:
        cmd = run.work.cmd
        # strip space characters
        cmd = cmd.lstrip()
        # extract the first argument of cmd
        args_list = cmd.rsplit(" ")
        while True:
            try:
                args_list.remove('')
            except Exception:
                break
        cmd_list.append(tuple(args_list))
        run_dict[tuple(args_list)] = run
    return (cmd_list, run_dict)

def parse_men(men, men_ip_dict):
    man_ip_list = []
    men_dict = {}
    
    for man in men:
        if man in men_ip_dict:
            ip = men_ip_dict[man]
            man_ip_list.append(ip)
            men_dict[ip] = man
            continue
        name = man.name.split('-')[0]
        if name[1] == 'o':  # hongo
            name += '.logos.ic.i.u-tokyo.ac.jp'
        elif name[1] == 'y':  # kyoto
            name += '.kyoto.intrigger.jp'
        ip = socket.gethostbyname(name)
        man_ip_list.append(ip)
        men_dict[ip] = man
        men_ip_dict[man] = ip
    return (man_ip_list, men_dict)


class MogamiJobScheduler():
    """
    >>> scheduler = MogamiJobScheduler()
    """
    def __init__(self, feature_file_path, mds_host):
        self.active = True

        self.men_ip_dict = {}

        # for profile
        self.init_t = 0.0
        self.predict_t = 0.0
        self.choose_t = 0.0
        self.check_t = 0.0
        self.all_t = 0.0

        self.log = ''


        try:
            # insert command data (if possible)
            f = open(feature_file_path, 'r')
            feature_data = cPickle.loads(f.read())
            self.ap_dict = feature_data[0]
            self.arg_job_dict = feature_data[1]
            f.close()
        except Exception, e:
            self.active = False

        # now get file location by asking to metadata server
        try:
            self.m_channel = MogamiChanneltoFileAsk(mds_host)
        except Exception, e:
            self.active = False

    def choose_proper_node(self, runs, men):
        self.log = ''
        match_list = []   # return list

        start_t = time.time()

        # prepare for process
        (cmds, runs_dict) = parse_runs(runs)
        (candidates, men_dict) = parse_men(men, self.men_ip_dict)
        men_resource_dict = {}
        for man in men:
            men_resource_dict[man] = man.capacity_left['cpu']

        init_end_t = time.time()

        # get locations of files
        files_to_ask = []
        for cmd in cmds:
            expected_files_dict = file_from_feature(
                cmd, self.ap_dict,self.arg_job_dict,
                runs_dict[cmd].work.dirs[0])
            files_to_ask.extend(expected_files_dict.keys())
        files_to_ask = list(set(files_to_ask))  # remove duplicative elements
        file_location_dict = self.m_channel.req_fileask(files_to_ask)

        predict_end_t = time.time()

        # choose best node for each run
        best_node_dict = {}
        
        for cmd in cmds:
            expected_files_dict = file_from_feature(
                cmd, self.ap_dict,self.arg_job_dict,
                runs_dict[cmd].work.dirs[0])
            load_dict = {}  # value: expected file read size of each node
            for filename, load in expected_files_dict.iteritems():
                try:
                    dest_list = file_location_dict[filename]
                except KeyError, e:
                    continue
                if dest_list == None:
                    continue
                for dest in dest_list:
                    #self.log += '%s is expected to read: filename = %s, dest = %s, size = %d' % (
                    #cmd[0], filename, dest, load)
                    if dest in load_dict:
                        load_dict[dest] += load
                    else:
                        load_dict[dest] = load
                    
            ordered_men_list = []
            for dest, load in load_dict.iteritems():
                ordered_men_list.append((load, dest))
            ordered_men_list.sort()
            ordered_men_list.reverse()
            for (load, man) in ordered_men_list:
                if man in candidates:
                    best_node_dict[runs_dict[cmd]] = men_dict[man]
                    break
            else:
                best_node_dict[runs_dict[cmd]] = None
            if cmd[0] == 'mAdd' and '133.50.19.7' in candidates:
                best_node_dict[runs_dict[cmd]] = men_dict['133.50.19.7']

        choose_end_t = time.time()

        # check resource and make matches run and man
        left_run = []
        for run, man in best_node_dict.iteritems():
            if man == None:
                left_run.append(run)
                continue
            if men_resource_dict[man] >= run.work.requirement['cpu']:
                match_list.append((run, man))
                men_resource_dict[man] -= run.work.requirement['cpu']
                #self.log += 'Choose Proper Node!! (%s)' % (man.name)
            else:
                left_run.append(run)

        for run in left_run:
            for man in men:
                if men_resource_dict[man] >= run.work.requirement['cpu']:
                    match_list.append((run, man))
                    men_resource_dict[man] -= run.work.requirement['cpu']
                    break
            else:
                match_list.append((run, men[0]))
                men_resource_dict[man] -= run.work.requirement['cpu']

        end_t = time.time()

        # add time
        self.init_t += init_end_t - start_t
        self.predict_t += predict_end_t - init_end_t
        self.choose_t += choose_end_t - predict_end_t
        self.check_t += end_t - choose_end_t
        self.all_t += end_t - start_t
            
        return match_list

    def get_former_log(self, ):
        buf = "++ match ++ %s" % (self.log, )
        return buf

    def get_times_str(self, ):
        buf = "++ scheduling time ++ [init]%f [predict]%f [choose]%f [check]%f [all]%f" % (
            self.init_t, self.predict_t, self.choose_t, self.check_t, self.all_t)
        return buf

if __name__ == '__main__':
    import doctest
    doctest.testmod()
