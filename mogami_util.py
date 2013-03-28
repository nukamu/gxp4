#! /usr/bin/env python

import os.path,socket,cPickle,cStringIO,time,sys

# TODO: should be retrieved from system automatically
MOGAMI_MOUNT = "/data/local2/mikity/mnt"

class MogamiChanneltoMeta():
    # decided statically (tentative?)
    REQ_FILEASK = 30
    REQ_INFO = 38
    REQ_DIRTREE = 39

    meta_port = 15806
    bufsize = 1024
    def __init__(self, mds_host):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        self.sock.connect((mds_host, self.meta_port))

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
        """request to ask location of files.
        
        @param file_list the list of files to ask the locations
        """
        self.send_msg((self.REQ_FILEASK, file_list))
        ans = self.recv_msg()
        return ans

    def req_info(self, ):
        """request to ask system infomation.
        
        @param 
        """
        self.send_msg((self.REQ_INFO))
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
            self.m_channel = MogamiChanneltoMeta(mds_host)
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
            expected_files_dict = self.file_predict(
                cmd, self.ap_dict,self.arg_job_dict,
                runs_dict[cmd].work.dirs[0])
            files_to_ask.extend(expected_files_dict.keys())
        files_to_ask = list(set(files_to_ask))  # remove duplicative elements
        file_location_dict = self.m_channel.req_fileask(files_to_ask)

        predict_end_t = time.time()

        # choose best node for each run
        best_node_dict = {}
        
        for cmd in cmds:
            expected_files_dict = self.file_predict(
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
                    self.log += "%s is expected to read: filename = %s," % \
                        (cmd[0], filename)
                    self.log += "dest = %s, size = %d" % (dest, load)
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
                    if runs_dict[cmd] not in best_node_dict:
                        best_node_dict[runs_dict[cmd]] = []
                    best_node_dict[runs_dict[cmd]].append(men_dict[man])
                    break
            else:
                best_node_dict[runs_dict[cmd]] = None

        choose_end_t = time.time()

        # check resource and make matches run and man
        left_run = []
        for run, man_ordered_list in best_node_dict.iteritems():
            if man == None:
                left_run.append(run)
                continue
            for man in man_ordered_list:
                if men_resource_dict[man] >= run.work.requirement['cpu']:
                    match_list.append((run, man))
                    men_resource_dict[man] -= run.work.requirement['cpu']
                    break
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

    def file_predict(self, cmd, feature_dict, arg_job_dict, cwd):
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
                            filename = cmd[counter + 1][:start + 1] + \
                                plus_str + cmd[counter + 1][-end:]
                            filename = os.path.join(cwd, filename)
                            filename = os.path.normpath(filename.replace(
                                    MOGAMI_MOUNT, ""))

                            if filename not in ret_file_dict:
                                ret_file_dict[filename] = size
                            else:
                                ret_file_dict[filename] += size
                        counter += 1
        return ret_file_dict


    def get_former_log(self, ):
        buf = "++ match ++ %s" % (self.log, )
        return buf

    def get_times_str(self, ):
        buf = "++ scheduling time ++ [init]%f [predict]%f [choose]%f [check]%f [all]%f" % (
            self.init_t, self.predict_t, self.choose_t, self.check_t, self.all_t)
        return buf
    

class mogami_html_generator:
    def __init__(self, interpreter, opts):
        self.interpreter = interpreter
        self.opts = opts

        print opts.meta
        self.m_channel = MogamiChanneltoMeta(opts.meta)

    def mk_basic_table(self, ):
        elements = ['metadata server host',
                    'start time',
                    'running time',
                    '# of data servers',
                    '# of clients',
                    ]
        v = ""
        mogami_status = self.interpreter.session.mogami_status
        d = {}
        d['metadata server host'] = mogami_status.meta_peer
        d['start time'] = time.strftime("%Y-%m-%d %H:%M:%S", 
                                        time.localtime(mogami_status.start_t))
        d['running time'] = "%.3f" % (time.time() - mogami_status.start_t, )
        d['# of data servers'] = len(mogami_status.data_peers)
        d['# of clients'] = len(mogami_status.fs_peers)
        for ele in elements:
            v += "<tr><td>%s</td><td>%s</tr>" % (ele, d[ele])
        v += "\n"
        return v

    def mk_conf_table(self, ):
        elements = ['meta',
                    'meta_dir',
                    'data_dir',
                    'web_dir',
                    'conf'
                    ]
        v = ""
        mogami_conf = self.interpreter.session.mogami_status.conf
        for ele in elements:
            attr = getattr(mogami_conf, ele)
            if attr is None:
                attr = " - "
            v += "<tr><td>%s</td><td>%s</td></tr>\n" % (ele, attr)
        v += "\n"
        return v

    def mk_meta_table(self, ):
        mogami_status = self.interpreter.session.mogami_status
        elements = ['host',
                    'meta_dir',
                    'tid',
                    'meta type',
                    '# of files',
                    'storage'
                    ]
        d = {}  # status dict
        d['host'] = mogami_status.meta_peer
        d['meta_dir'] = mogami_status.meta_dir
        d['tid'] = mogami_status.meta_tid
        d['meta type'] = 'fs'
        d['# of files'] = 0
        d['storage'] = 0
        v = ""

        for ele in elements:
            v += "<tr><td>%s</td><td>%s</tr>" % (ele, d[ele])
        v += "\n"
        return v

    def mk_data_table(self, ):
        mogami_status = self.interpreter.session.mogami_status
        elements = ['host',
                    'data_dir',
                    'tid',
                    '# of files',
                    'storage'
                    ]
        v = ""
        v += "<tr><td>id</td>"
        for ele in elements:
            v += "<td>%s</td>" % (ele)
        v += "</tr>"
        for i, data in enumerate(mogami_status.data_peers):
            v += "<tr><td>%d</td>" % (i)
            v += "<td>%s</td><td>%s</td>" % \
                (data, mogami_status.data_dir_d[data])
            v += "<td>%s</td><td>%s</td>" % (mogami_status.data_tid, 100)
            v += "<td>30% (2G/6G)</td></tr>"
        return v

    def mk_client_table(self, ):
        mogami_status = self.interpreter.session.mogami_status
        elements = ['host',
                    'mnt_dir'
                    ]
        v = ""
        v += "<tr><td>id</td>"
        for ele in elements:
            v += "<td>%s</td>" % (ele)
        v += "</tr>"
        for i, host in enumerate(mogami_status.fs_peers):
            v += "<tr><td>%d</td>" % (i)
            v += "<td>%s</td><td>%s</td>" % \
                (host, mogami_status.fs_dir_d[host])
        return v

    def generate(self):
        D = {}
        reload_directive = ""
        D["reload_directive"] = reload_directive
        D["basic_info_table"] = self.mk_basic_table()
        D["conf_table"] = self.mk_conf_table()
        D["metadata_server_table"] = self.mk_meta_table()
        D["data_servers_table"] = self.mk_data_table()
        D["clients_table"] = self.mk_client_table()
        ttt = int(time.time() * 10)
        html = os.path.join(self.opts.web_dir, "index.html")
        html_t = os.path.join(self.opts.web_dir, "index_%10d.html" % ttt)
        template_html = "${GXP_DIR}/gxpbin/mogami_template.html"
        template_html = os.path.expanduser(os.path.expandvars(template_html))
        wp = open(html_t, "wb")
        fp = open(template_html, "rb")
        template = fp.read()
        fp.close()
        wp.write(template % D)
        wp.flush()
        wp.close()
        os.rename(html_t, html)


class mogami_env:
    def __init__(self):
        self.conf = None
        self.start_t = time.time()
        
        # metadata server info
        self.meta_peer = None
        self.meta_dir = ""
        self.meta_tid = -1

        # data servers info
        self.data_peers = []
        self.data_dir_d = {}  # k: peer, v: dirname
        self.data_tid = -1

        # clients info
        self.fs_peers = []        
        self.fs_dir_d = {}  # k: peer, v: dirname

    def show(self):
        """create texts for representing mogami's current status.
        """
        ret_str = "meta=%s,data=%s,client=%s" % (str(meta_peer), 
                                                 str(data_peers),
                                                 str(fs_peers))  # tentatively
        return ret_str

    def set_conf(self, conf):
        """
        """
        self.conf = conf

    def set_mogami_meta(self, meta, meta_dir, tid):
        """
        """
        self.meta_peer = meta
        self.meta_dir = meta_dir
        self.meta_tid = tid

    def set_mogami_data(self, data_list, data_dir, tid):
        """
        """
        for data in data_list:
            if data in self.data_dir_d:
                ret_str = "cannot set multiple data servers on single host"
                sys.exit(ret_str + " (host: %s)" % (data, ))
            self.data_peers.append(data)
            self.data_dir_d[data] = data_dir
        self.data_tid = tid

    def set_mogami_fs(self, client_list, mnt_dir):
        """
        """
        for client in client_list:
            if client in self.fs_dir_d:
                ret_str = "cannot set multiple clients on single host"
                sys.exit(ret_str + " (tentatively) (host: %s)" % (data, ))
            self.fs_peers.append(client)
            self.fs_dir_d[client] = mnt_dir

