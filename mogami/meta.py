#! /usr/bin/env python
#-*- coding: utf-8 -*-

from __future__ import with_statement

# import python standard modules
import os
import os.path
import sys
import errno
import socket
import select
import Queue
import string
import random

# import mogami's original modules
import channel
import metadata
import daemons
import conf
from system import MogamiLog


class MogamiSystemInfo(object):
    """This object should be held by metadata server.
    """
    def __init__(self, meta_rootpath):
        self.meta_rootpath = os.path.abspath(meta_rootpath)

        # information of data servers
        self.data_list = []
        self.data_rootpath = {}

        self.ramfile_list = []
        self.delfile_q = Queue.Queue()
        self.repfile_q = Queue.Queue()

    def add_data_server(self, ip, rootpath):
        """append a data server to Mogami system

        @param ip ip address of node to append
        @param rootpath root directory path of the data server
        """
        if not ip in self.data_list:
            self.data_list.append(ip)
            self.data_rootpath[ip] = rootpath

    def get_data_rootpath(self, ip):
        """
        """
        try:
            return self.data_rootpath[ip]
        except KeyError, e:
            return None

    def choose_data_server(self, dest):
        """choose destination of new file.

        if dest exists in data servers, dest will be returned.
        @param dest
        """
        rand = 0
        if len(self.data_list) <= 0:
            return None
        if dest in self.data_list:
            return dest
        rand = random.randint(0, len(self.data_list) - 1)
        return self.data_list[rand]

    def remove_data_server(self, ip):
        """remove a data server from Mogami system

        @param ip ip address of node to remove
        @return 0 with success, -1 with error
        """
        try:
            self.data_list.remove(ip)
            del self.data_rootpath[ip]
            return True
        except Exception, e:
            MogamiLog.error("cannot find %s from data servers" % (ip))
            return False

    def register_ramfiles(self, add_files_list):
        """

        @param add_files_list
        @return
        """
        self.ramfile_list.extend(add_files_list)

    def add_delfile(self, dest, data_path):
        """
        @param dest 
        @param data_path
        """
        self.delfile_q.put((dest, data_path))

    def add_repfile(self, path, org, org_data_path, dest, dest_data_path):
        """
        """
        self.repfile_q.put((path, org, org_data_path, dest, dest_data_path))


class MogamiDaemonOnMeta(daemons.MogamiDaemons):
    """A daemon on the metadata server.

    handle file {replication, deletion} requests.
    """
    def __init__(self, sysinfo, meta_rep):
        MogamiLog.debug("== start daemon on metadata server ==")
        daemons.MogamiDaemons.__init__(self)
        self.meta_rep = meta_rep
        self.delfile_q = sysinfo.delfile_q
        self.repfile_q = sysinfo.repfile_q
        self.sock_list =[]
        self.sock_dict = {}

    def run(self, ):
        while True:
            # check deletion requests
            try:
                (target_ip, target_file) = self.delfile_q.get(timeout=0.01)
                self.send_delete_request(target_ip, [target_file, ])
            except Queue.Empty:
                pass

            # check replication requests
            try:
                (path, org, org_path, dest, 
                 dest_path) = self.repfile_q.get(timeout=0.01)
                self.send_replication_request(path, org, org_path, 
                                              dest, dest_path)
            except Queue.Empty:
                pass

            # check replication end messages
            ch_list = select.select(self.sock_list, [], [], 0.01)[0]
            for sock_id in ch_list:
                (ch, path, org, org_path,
                 dest, dest_path) = self.sock_dict[sock_id]
                (ans, f_size) = ch.filerep_getanswer()
                if ans == 0:
                    # add new location to metadata 
                    self.meta_rep.addrep(path, dest, dest_path, f_size)
                self.sock_list.remove(sock_id)
                del self.sock_dict[sock_id]

    def send_delete_request(self, ip, files):
        MogamiLog.debug("file delete request was sent (%s -> %s)" %
                        (ip, str(files)))
        c_channel = channel.MogamiChanneltoData(ip)
        ans = c_channel.delfile_req(files)
        c_channel.close_req()
        c_channel.finalize()

    def send_replication_request(self, path, org, org_path, dest, dest_path):
        MogamiLog.debug("file replication request was sent (%s: %s -> %s: %s)" %
                        (org, org_path, dest, dest_path))

        c_channel = channel.MogamiChanneltoData(org)
        c_channel.filerep_req(path, org_path, dest, dest_path)

        sock_id = c_channel.sock.fileno()
        self.sock_list.append(sock_id)
        self.sock_dict[sock_id] = (c_channel, path, org, org_path,
                                   dest, dest_path)

class MogamiMetaHandler(daemons.MogamiRequestHandler):
    """This is the class for thread created for each client.
    This handler is run as multithread.
    """
    def __init__(self, client_channel, sysinfo, meta_rep):
        daemons.MogamiRequestHandler.__init__(self)
        self.sysinfo = sysinfo
        self.c_channel = client_channel

        if meta_rep == None:  # case of 'db'
            self.meta_rep = metadata.MogamiMetaDB(self.sysinfo.meta_rootpath)
        else:
            self.meta_rep = meta_rep

        req_types = [(channel.REQ_GETATTR, 'getattr'),
                     (channel.REQ_READDIR, 'readdir'),
                     (channel.REQ_ACCESS, 'access'),
                     (channel.REQ_MKDIR, 'mkdir'),
                     (channel.REQ_RMDIR, 'rmdir'),
                     (channel.REQ_UNLINK, 'unlink'),
                     (channel.REQ_RENAME, 'rename'),
                     (channel.REQ_MKNOD, 'mknod'),
                     (channel.REQ_CHMOD, 'chmod'),
                     (channel.REQ_CHOWN, 'chown'),
                     (channel.REQ_LINK, 'link'),
                     (channel.REQ_SYMLINK, 'symlink'),
                     (channel.REQ_READLINK, 'readlink'),
                     (channel.REQ_TRUNCATE, 'truncate'),
                     (channel.REQ_UTIME, 'utime'),
                     (channel.REQ_FSYNC, 'fsync'),
                     (channel.REQ_OPEN, 'open'),
                     (channel.REQ_RELEASE, 'release'),
                     (channel.REQ_FGETATTR, 'fgetattr'),
                     (channel.REQ_FTRUNCATE, 'ftruncate'),
                     (channel.REQ_DATAADD, 'dataadd'),
                     (channel.REQ_DATADEL, 'datadel'),
                     (channel.REQ_RAMFILEADD, 'register_ramfiles'),
                     (channel.REQ_FILEASK, 'fileask'),
                     (channel.REQ_FILEREP, 'filerep')
                     ]

        for (req, funcname) in req_types:
            self.regist_handler(req, funcname)

    # MogamiSystem APIs
    def dataadd(self, rootpath):
        ip = self.c_channel.getpeername()
        
        self.sysinfo.add_data_server(ip, rootpath)
        MogamiLog.info("delete data server IP: %s" % ip)
        MogamiLog.info("Now there are %d data servers." %
                       len(self.sysinfo.data_list))

    def datadel(self, ):
        ip = self.c_channel.getpeername()
        ret = self.sysinfo.remove_data_server(ip)

        if ret == True:
            MogamiLog.info("delete data server IP: %s" % ip)
            MogamiLog.info("Now there are %d data servers." %
                           len(self.sysinfo.data_list))

    def register_ramfiles(self, add_file_list):
        """register files in list to files to manage on memory

        @param add_file_list
        """
        ramfile_list.extend(add_file_list)

        MogamiLog.debug("** register ramfiles **")
        MogamiLog.debug("add files = " + str(add_file_list))

    def remove_ramfiles(self, file_list):
        """not implemented yet..
        """
        pass

    # Mogami's actual metadata access APIs
    def getattr(self, path):
        MogamiLog.debug("path = %s" % path)
        try:
            st_dict = self.meta_rep.getattr(path)
            ans = 0
        except os.error, e:
            MogamiLog.debug("stat error!")
            ans = e.errno
            st_dict = None
        self.c_channel.getattr_answer(ans, st_dict)

    def readdir(self, path, offset):
        MogamiLog.debug('path=%s' % (path))
        try:
            l = self.meta_rep.readdir(path)
            ans = 0
            MogamiLog.debug("result = %s" % (str(l)))
        except os.error, e:
            l = None
            ans = e.errno
            MogamiLog.debug("readdir error")

        self.c_channel.readdir_answer(ans, l)

    def access(self, path, mode):
        MogamiLog.debug("path = %s" % (path))
        try:
            if self.meta_rep.access(path, mode) == True:
                ans = 0
            else:
                ans = errno.EACCES
        except os.error, e:
            ans = e.errno

        self.c_channel.access_answer(ans)

    def mkdir(self, path, mode):
        MogamiLog.debug("path = %s mode = %o" % (path, mode))
        try:
            self.meta_rep.mkdir(path, mode)
            ans = 0
        except os.error, e:
            ans = e.errno
        self.c_channel.mkdir_answer(ans)

    def rmdir(self, path):
        MogamiLog.debug("path=%s" % (path))
        try:
            self.meta_rep.rmdir(path)
            ans = 0
        except os.error, e:
            ans = e.errno
        self.c_channel.rmdir_answer(ans)

    def unlink(self, path, async):
        MogamiLog.debug("path = %s" % path)
        try:
            (dest, dest_path) = self.meta_rep.unlink(path)
            if async is True:
                ans = 0
                if dest != None:
                    self.sysinfo.add_delfile(dest, dest_path)
            else:
                c_channel = channel.MogamiChanneltoData(dest)
                ans = c_channel.delfile_req([dest_path, ])
                c_channel.close_req()
                c_channel.finalize()
        except os.error, e:
            ans = e.errno
        except Exception, e:
            ans = e.errno
            MogamiLog.error("cannot remove file contents of %s" % path)

        self.c_channel.unlink_answer(ans)

    def rename(self, oldpath, newpath):
        MogamiLog.debug(oldpath + ' -> ' + newpath)
        try:
            self.meta_rep.rename(oldpath, newpath)
            ans = 0
        except os.error, e:
            ans = e.errno
        self.c_channel.rename_answer(ans)

    def chmod(self, path, mode):
        MogamiLog.debug("path = %s w/ mode %s" % (path, oct(mode)))
        try:
            self.meta_rep.chmod(path, mode)
            ans = 0
        except os.error, e:
            ans = e.errno
        self.c_channel.chmod_answer(ans)

    def chown(self, path, uid, gid):
        MogamiLog.debug("path=%s uid=%d gid=%d" % (path, uid, gid))
        try:
            self.meta_rep.chown(path, uid, gid)
            ans = 0
        except os.error, e:
            ans = e.errno
        self.c_channel.chown_answer(ans)

    def symlink(self, frompath, topath):
        MogamiLog.debug("frompath = %s, topath = %s" % (frompath, topath))
        try:
            self.meta_rep.symlink(frompath, topath)
            ans = 0
        except os.error, e:
            ans = e.errno
        self.c_channel.symlink_answer(ans)

    def readlink(self, path):
        MogamiLog.debug("path = %s" % path)
        try:
            result = self.meta_rep.readlink(path)
            ans = 0
        except os.error, e:
            ans = e.errno
            result = None
        self.c_channel.readlink_answer(ans, result)

    def truncate(self, path, length):
        """truncate handler.

        @param path file path to truncate
        @param length length of output file
        """
        MogamiLog.debug("path = %s, length = %d" % (path, length))
        try:
            (dest, data_path) = self.meta_rep.truncate(path, length)
            ans = 0
        except IOError, e:
            ans = e.errno
            dest = None
            data_path = None
        except Exception, e:
            ans = e.errno
            dest = None
            data_path = None

        self.c_channel.truncate_answer(ans, dest, data_path)

    def utime(self, path, times):
        MogamiLog.debug("path = %s, times = %s" % (path, str(times)))
        try:
            self.meta_rep.utime(path, times)
            ans = 0
        except os.error, e:
            ans = e.errno
        self.c_channel.utime_answer(ans)

    def open(self, path, flag, mode):
        """open handler.

        @param path file path
        @param flag flags for open(2)
        @param mode open mode (may be empty tuple): actual value is mode[0]
        """
        if self.meta_rep.access(path, os.F_OK) == True:
            # When the required file exist...
            try:
                MogamiLog.debug("!!find the file %s w/ %o" % (path, flag))
                affinity = self.c_channel.getpeername()
                (dest, data_path, fsize) = self.meta_rep.open(
                    path, flag, mode, affinity)

                # create data to send
                ans = 0
                created = False
            except Exception, e:
                MogamiLog.debug("!!find the file but error for %s (%s)" %
                                (path, e))
                try:
                    ans = e.errno
                except Exception, e:
                    ans = errno.ENOENT
                dest = None
                fd = None
                data_path = None
                fsize = None
                created = False
        else:
            # creat new file
            MogamiLog.debug("can't find the file so create!!")
            try:
                fsize = 0

                # determine (dest, data_path)
                dest = self.sysinfo.choose_data_server(
                    self.c_channel.getpeername())
                if dest == None:
                    MogamiLog.error("!! There are no data server to create file !!")
                filename = ''.join(random.choice(string.letters)
                                   for i in xrange(16))
                data_path = os.path.join(
                    self.sysinfo.get_data_rootpath(dest), filename)
                
                self.meta_rep.create(path, flag, mode, dest, data_path)
                MogamiLog.debug("filename is %s" % (data_path,))
                created = True
                ans = 0
            except Exception, e:
                MogamiLog.error("!! have fatal error @2!! (%s)" % (e))
                try:
                    ans = e.errno
                except Exception, e:
                    ans = errno.ENOSYS
                dest = None
                fd = None
                data_path = None
                fsize = None
                created = False

        # case of client has file data
        if dest == self.c_channel.getpeername():
            dest = "self"

        self.c_channel.open_answer(ans, dest, data_path, fsize, created)

    def release(self, path, fsize):
        """release handler.

        @param fd file discripter
        @param writelen size of data to be written
        """
        try:
            self.meta_rep.release(path, fsize)
        except Exception, e:
            MogamiLog.error("OSError in release (%s)" % (e))
        ans = 0
        self.c_channel.release_answer(ans)

        if conf.auto_repl == True:
            filename = os.path.basename(path)
            if filename.find('fit.') == 0:
                (org, org_path, fsize) = self.meta_rep._get_metadata_one(path)
                
                f_name = os.path.basename(org_path)  # extract only file name
                dest_path = os.path.join(self.sysinfo.get_data_rootpath('133.50.19.7'),
                                         f_name)
                self.sysinfo.add_repfile(path, org, org_path, '133.50.19.7', dest_path)
            elif filename.find('c2mass') == 0:
                (org, org_path, fsize) = self.meta_rep._get_metadata_one(path)
                
                f_name = os.path.basename(org_path)  # extract only file name
                dest_path = os.path.join(self.sysinfo.get_data_rootpath('133.50.19.7'),
                                         f_name)
                self.sysinfo.add_repfile(path, org, org_path, '133.50.19.7', dest_path)

    def fgetattr(self, fd):
        try:
            st = os.fstat(fd)
            senddata = [0, st]
        except os.error, e:
            MogamiLog.debug("OSError in fgetattr (%s)" % (e))
            senddata = [e.errno, 'null']
        self.c_channel.fgetattr_answer(senddata)

    def file_ask(self, path_list):
        dest_dict = {}
        for path in path_list:
            try:
                file_dict = self.meta_rep._get_metadata(path)
                for dest in file_dict.iterkeys():                    
                    if path not in dest_dict:
                        dest_dict[path] = []
                    dest_dict[path].append(dest)
            except Exception:
                dest_dict[path] = None
        self.c_channel.fileask_answer(dest_dict)

    def file_rep(self, path, dest):
        (org, org_path, fsize) = self.meta_rep._get_metadata_one(path)

        f_name = os.path.basename(org_path)  # extract only file name
        dest_path = os.path.join(self.sysinfo.get_data_rootpath(dest),
                                 f_name)
        self.sysinfo.add_repfile(path, org, org_path, dest, dest_path)


class MogamiMeta(object):
    """This is the class of mogami's metadata server
    """
    def __init__(self, rootpath):
        """This is the function of MogamiMeta's init.
        """
        MogamiLog.init(MogamiLog.TYPE_META, conf.meta_loglevel)

        self.sysinfo = MogamiSystemInfo(rootpath)

        # create a object to retrieve metadata information (meta repository)
        if conf.meta_type == 'fs':
            self.meta_rep = metadata.MogamiMetaFS(rootpath)
        elif conf.meta_type == 'db':
            self.meta_rep = None
        else:
            sys.exit("meta_type should be 'fs' or 'db' in config file")

        MogamiLog.info("** Mogami metadata server init was completed **")
        MogamiLog.debug("rootpath = " + rootpath)

    def run(self, ):
        # listen for being connected from Mogami client.
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lsock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.lsock.bind(("0.0.0.0", conf.metaport))
        self.lsock.listen(10)
        MogamiLog.debug("Listening at the port " + str(conf.metaport))
        daemons_list = []
        thread_collector = daemons.MogamiThreadCollector(daemons_list)
        thread_collector.start()
        threads_count = 0

        meta_daemon_thread = MogamiDaemonOnMeta(self.sysinfo, self.meta_rep)
        meta_daemon_thread.start()

        while True:
            (client_sock, address) = self.lsock.accept()
            MogamiLog.debug("accept connnect from %s" % (str(address[0])))
            client_channel = channel.MogamiChannelforMeta()
            client_channel.set_socket(client_sock)
            metad = MogamiMetaHandler(client_channel,
                                      self.sysinfo, self.meta_rep)
            metad.start()
            daemons_list.append(metad)

            MogamiLog.debug("Created thread name = " + metad.getName())


def main(dir_path, config=None):
    meta = MogamiMeta(dir_path)
    meta.run()


if __name__ == '__main__':
    main(sys.argv[1])
