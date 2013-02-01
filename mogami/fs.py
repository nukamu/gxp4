#! /usr/bin/env python
#-*- coding: utf-8 -*-

from __future__ import with_statement

from fuse import Fuse
import fuse
fuse.fuse_python_api = (0, 2)

# python standard modules
import stat
import errno
import os
import os.path
import Queue
import sys
import time
import threading

# mogami's original modules
import conf
import channel
import system
import daemons
import tips
import filemanager
from system import MogamiLog


m_channel = channel.MogamiChanneltoMeta()
daemons_list = []
file_size_dict = {}
channels = channel.MogamiChannelRepository()
file_access_dict = {}
file_access_lock = threading.Lock()
local_ch = channel.MogamiChanneltoData()

negative_cache = {}

class MogamiFileAccessHistory(object):
    def __init__(self, ):
        self.ap_dict = {}
        self.ap_lock = threading.Lock()

    def put_ap(self, ap, pid, parent_list):
        with self.ap_lock:
            if pid not in self.ap_dict:
                self.ap_dict[pid] = []
            self.ap_dict[pid].append(ap)

            for ppid in parent_list:
                if ppid not in self.ap_dict:
                    self.ap_dict[ppid] = []
                self.ap_dict[ppid].append(ap)

    def get_ap(self, pid):
        with self.ap_lock:
            if pid in self.ap_dict:
                return self.ap_dict[pid]
            else:
                return []

file_access_rep = MogamiFileAccessHistory()

class MogamitoTellAccessPattern(daemons.MogamiDaemons):
    """Class for answer access pattern history.
    """
    def __init__(self, pipepath):
        daemons.MogamiDaemons.__init__(self)
        self.pipepath = pipepath
        if os.access(self.pipepath, os.F_OK) == True:
            os.remove(self.pipepath)
        self.channel = channel.MogamiChanneltoTellAP(self.pipepath)

    def run(self, ):
        """This thread should answer questions about file access pattern from gxpd process.
        """
        while True:
            self.answer_to_client()

    def answer_to_client(self, ):
        """
        """
        c_channel = self.channel.accept_with_timeout(10.0)
        if c_channel == None:
            return None
        pid = c_channel.recv_msg()
        ap = file_access_rep.get_ap(pid)
        c_channel.send_msg(ap)
        c_channel.finalize()


class MogamiFS(Fuse):
    """Class for Mogami file system (client)
    """
    def __init__(self, meta_server, *args, **kw):
        Fuse.__init__(self, *args, **kw)
        self.meta_server = meta_server
        self.parse(errex=1)
        m_channel.connect(self.meta_server)

    def fsinit(self, ):
        """Called before fs.main() called.
        """
        # initializer log
        MogamiLog.info("** Mogami FS init **")
        MogamiLog.debug("Success in creating connection to metadata server")
        MogamiLog.debug("Init complete!!")
        
        # create a thread for collecting dead threads
        collector_thread = daemons.MogamiThreadCollector(daemons_list)
        collector_thread.start()

        # create a thread for telling access pattern logs
        tellap_thread = MogamitoTellAccessPattern('/tmp/mogami_ap')
        daemons_list.append(tellap_thread)
        tellap_thread.start()

    def finalize(self, ):
        """Finalizer of Mogami.
        This seems not to be called implicitly...
        """
        m_channel.finalize()
        MogamiLog.info("** Mogami Unmount **")

    # From here functions registered for FUSE are written.
    def mythread(self):
        MogamiLog.debug("** mythread **")
        return -errno.ENOSYS

    def getattr(self, path):
        MogamiLog.debug("** getattr ** path = %s" % (path, ))

        if path in negative_cache:
            if time.time() - negative_cache[path] < 3.0:
                return -errno.ENOENT

        (ans, st_dict) = m_channel.getattr_req(path)
        if ans != 0:
            if ans == errno.ENOENT and path.find('.pm') != -1:                
                negative_cache[path] = time.time()
            return -ans
        else:
            st = filemanager.MogamiStat()
            st.load(st_dict)
        # if file_size_dict has cache of file size, replace it
        if path in file_size_dict:
            st.chsize(file_size_dict[path])
        return st

    def readdir(self, path, offset):
        MogamiLog.debug("** readdir ** path = %s, offset = %s" %
                        (path, str(offset)))

        (ans, contents) = m_channel.readdir_req(path, offset)
        l = ['.', '..']
        if ans == 0:
            l.extend(contents)
            return [fuse.Direntry(ent) for ent in l]
        else:
            return -ans

    def access(self, path, mode):
        """access handler.

        @param path path to access
        @param mode mode to access
        @return 0 on success, errno on error
        """
        MogamiLog.debug("** access **" + path + str(mode))
        ans = m_channel.access_req(path, mode)
        return -ans

    def mkdir(self, path, mode):
        """mkdir handler.
        
        @param path directory path to mkdir
        @param mode permission of the directory to create
        @return 0 on success, errno on error
        """
        MogamiLog.debug("** mkdir **" + path + str(mode))
        ans = m_channel.mkdir_req(path, mode)
        return -ans

    def rmdir(self, path):
        """rmdir handler.
        """
        MogamiLog.debug("** rmdir **" + path)
        ans = m_channel.rmdir_req(path)
        return -ans

    def unlink(self, path):
        """unlink handler.

        @param path path name to unlink
        """
        MogamiLog.debug("** unlink ** path = %s" % (path, ))
        if conf.local_request is False:
            ans = m_channel.unlink_req(path, True)
        else:
            ans = m_channel.unlink_req(path, False)
        return -ans

    def rename(self, oldpath, newpath):
        """rename handler.

        @param oldpath original path name before rename
        @param newpath new path name after rename
        """
        MogamiLog.debug("** rename ** oldpath = %s, newpath = %s" %
                        (oldpath, newpath))
        ans = m_channel.rename_req(oldpath, newpath)
        if ans != 0:
            return -ans

    def chmod(self, path, mode):
        """chmod handler.

        @param path path to change permission of
        @param mode permission to change
        """
        MogamiLog.debug("** chmod ** path = %s, mode = %s" %
                        (path, oct(mode)))
        ans = m_channel.chmod_req(path, mode)
        if ans != 0:
            return -ans

    def chown(self, path, uid, gid):
        """chown handler.

        @param path path to change owner of
        @param uid user id of new owner
        @param gid group id of new owner
        """
        MogamiLog.debug('** chown ** ' + path + str(uid) + str(gid))
        ans = m_channel.chown_req(path, uid, gid)
        return -ans

    def symlink(self, frompath, topath):
        """symlink handler.

        @param frompath 
        @param topath
        """
        MogamiLog.debug("** symlink ** frompath = %s, topath = %s" %
                        (frompath, topath))
        ans = m_channel.symlink_req(frompath, topath)
        return -ans

    def readlink(self, path):
        """readlink handler.

        @param path path to read link of
        @return result path of readlink with success, errno with error
        """
        MogamiLog.debug("** readlink ** path = %s" % (path))
        (ans, result) = m_channel.readlink_req(path)
        if ans != 0:
            return -ans
        return result

    def truncate(self, path, length):
        """truncate handler.

        @param path path of file to truncate
        @param length expected file length after truncate
        @return 0 with success, errno with error
        """
        MogamiLog.debug('** truncate ** path = %s, length = %d' %
                        (path, length))

        (ans, dest, filename) = m_channel.truncate_req(path, length)
        if ans != 0:
            return -ans

        c_channel = channel.MogamiChanneltoData(dest)
        ans = c_channel.truncate_req(filename, length)
        c_channel.finalize()

        # if truncate was succeeded, cache of file size should be changed
        if ans == 0:
            file_size_dict[path] = length
        return -ans

    def utime(self, path, times):
        MogamiLog.debug('** utime **' + path + str(times))
        ans = m_channel.utime_req(path, times)
        if ans != 0:
            return -ans

    class MogamiFile(Fuse):
        """This is the class of file management on Mogami.
        """

        def __init__(self, path, flag, *mode):
            """Initializer called when opened.

            @param path file path
            @param flag flags with open(2)
            @param *mode file open mode (may not be specified)
            """
            MogamiLog.debug("** open ** path = %s, flag = %s, mode = %s" %
                            (path, str(flag), str(mode)))
            if conf.ap is True:
                start_t = time.time()

            # parse argurments
            self.path = path
            self.flag = flag
            self.mode = mode

            dest = None
            self.local = False

            if conf.local_request is True:
                try:
                    if local_ch.connected == False:
                        local_ch.connect('127.0.0.1')
                    (ans, data_path, fsize) = local_ch.filereq_req(path)
                    if ans == 0:  # exists on local
                        dest = 'self'
                        self.local = True
                        self.created = False
                        self.fsize = fsize
                except Exception:
                    pass

            if dest == None:
                # request for metadata server
                (ans, dest, data_path, self.fsize,
                 self.created) = m_channel.open_req(path, flag, *mode)
                if dest == 'self':
                    if local_ch.connected == False:
                        local_ch.connect('127.0.0.1')
                    local_ch.fileadd_req(self.path, data_path)
                        
                if ans != 0:  # error on metadata server
                    e = IOError()
                    e.errno = ans
                    raise e

            if dest == 'self':
                self.local = True
                self.mogami_file = filemanager.MogamiLocalFile(
                    self.fsize, local_ch, path, data_path, flag, *mode)
            else:
                self.local = False
                self.mogami_file = filemanager.MogamiRemoteFile(
                    self.fsize, dest, path, data_path, flag, *mode)
                ans = self.mogami_file.create_connections(channels)
                if ans != 0:
                    MogamiLog.error("open error !!")
                    e = IOError()
                    e.errno = ans
                    raise e

            # register file size to file size dictionary
            file_size_dict[path] = self.fsize

            if conf.ap is True:
                """Get Id list to know pid.
                list = {gid: pid: uid}
                And then get the command from pid.
                """
                try:
                    id_list = self.GetContext()
                    pid = id_list['pid']
                    f = open(os.path.join("/proc", str(pid), "cmdline"), 'r')
                    cmd_args = f.read().rsplit('\x00')[:-1]
                except Exception, e:
                    # with any error, pass this proccess
                    cmd_args = None
                    pid = -1
                self.access_pattern = filemanager.MogamiAccessPattern(
                    path, cmd_args, pid)
                end_t = time.time()
                self.took_time = end_t - start_t
            else:
                id_list = self.GetContext()
                pid = id_list['pid']
                f = open(os.path.join("/proc", str(pid), "cmdline"), 'r')
                self.cmd = f.read().rstrip('\x00').replace('\x00', ' ')

            self.read_size = 0
            self.write_size = 0


        def read(self, length, offset):
            """read handler.

            @param length request size of read
            @param offset offset of read request
            @return data read from file (may return errno with error)
            """
            if conf.ap is True:
                start_t = time.time()

            MogamiLog.debug("**read offset=%d, length=%d" % (offset, length))

            ret_buf = self.mogami_file.read(length, offset)

            if conf.ap is True:
                end_t = time.time()
                self.access_pattern.insert_data(self.access_pattern.read, offset,
                                                len(ret_buf))
                
                self.took_time += end_t - start_t
            self.read_size += len(ret_buf)

            return ret_buf

        def write(self, buf, offset):
            """write handler.

            This function is NOT ensure to write to disk.
            @param buf data to write
            @param offset
            @return 0 with success, errno with error
            """
            if conf.ap is True:
                start_t = time.time()

            if self.fsize < offset + len(buf):
                self.fsize = offset + len(buf)
                file_size_dict[self.path] = self.fsize
            ret_value = self.mogami_file.write(buf, offset)

            if conf.ap is True and ret_value > 0:
                self.access_pattern.insert_data(self.access_pattern.write,
                                                offset, ret_value)
            if conf.ap is True:
                end_t = time.time()
                self.took_time += end_t - start_t
            self.write_size += ret_value
            return ret_value

        def flush(self, ):
            if conf.ap is True:
                start_t = time.time()

            # actual flush operation
            ans = self.mogami_file.flush()

            if conf.ap is True:
                end_t = time.time()
                self.took_time += end_t - start_t
            return ans

        def fsync(self, isfsyncfile):
            if conf.ap is True:
                start_t = time.time()
            
            # actual fsync operation
            ans = self.mogami_file.fsync(isfsyncfile)

            if conf.ap is True:
                end_t = time.time()
                self.took_time += end_t - start_t
            return ans

        def release(self, flags):
            if conf.ap is True:
                start_t = time.time()
            MogamiLog.debug("** release **")
            MogamiLog.critical("** file log ** file:%s\tcmd:%s\tlocal:%s\tread:%s\twrite:%s" % (
                self.path, self.cmd, str(self.local), str(self.read_size), str(self.write_size)))

            fsize = self.mogami_file.release(flags)
            ans = m_channel.release_req(self.path, fsize)
            # delete file size cache
            if self.path in file_size_dict:
                del file_size_dict[self.path]

            if conf.ap is True:
                # prepare data to tell access pattern
                myname = m_channel.getmyname()
                (read_data, write_data) = self.access_pattern.mk_form_data()
                pid = self.access_pattern.pid
                cmd_args = self.access_pattern.cmd_args
                path = self.access_pattern.path

                # get pids of parents
                parents_list = self.access_pattern.parents_list

                end_t = time.time()
                self.took_time += end_t - start_t
                
                # put file access history to repository
                if cmd_args != None:
                    file_access_rep.put_ap((cmd_args, pid, path, myname,
                                            self.took_time, self.created,
                                            read_data, write_data),
                                           pid, parents_list)
            return 0


    def main(self, *a, **kw):
        """This is the main method of MogamiFS.
        """
        self.file_class = self.MogamiFile
        return Fuse.main(self, *a, **kw)


if __name__ == "__main__":
    if 'big_writes' not in sys.argv:
        sys.argv.extend(['-o', 'big_writes'])
    if 'large_read' not in sys.argv:
        sys.argv.extend(['-o', 'large_read'])
    sys.argv.extend(['-o', 'kernel_cache'])
    MogamiLog.init("fs", conf.fs_loglevel)
    fs = MogamiFS(sys.argv[1],
                  version="%prog " + fuse.__version__,
                  usage=system.usagestr())
    fs.flags = 0
    fs.multithreaded = conf.multithreaded
    fs.main()
    fs.finalize()
