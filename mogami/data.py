#! /usr/bin/env python
#-*- coding: utf-8 -*-

from __future__ import with_statement

import os
import os.path
import sys
import errno
import string
import random
import threading
import re
import time
import stat
import socket
import cPickle
import cStringIO

import atexit    # for leave from meta data sever list

import channel
import daemons
import conf
from system import MogamiLog

class MogamiLocalFileDict(object):
    def __init__(self, ):
        self.filedict = {}
        self.lock = threading.Lock()
        
    def addfile(self, mogami_path, local_path):
        with self.lock:
            self.filedict[mogami_path] = local_path
    
    def delfile(self, mogami_path):
        with self.lock:
            del self.filedict[mogami_path]

    def checkfile(self, mogami_path):
        with self.lock:
            try:
                local_path = self.filedict[mogami_path]
            except KeyError:
                local_path = None
        return local_path
        
class MogamiDataHandler(daemons.MogamiDaemons):
    """This is the class for thread created for each client.
    This handler is run as multithread.
    """
    def __init__(self, c_channel, rootpath, filedict):
        daemons.MogamiDaemons.__init__(self)
        self.c_channel = c_channel
        self.rootpath = rootpath
        self.filedict = filedict

    def run(self, ):
        while True:
            req = self.c_channel.recv_request()
            if req == None:
                MogamiLog.debug("Connection closed")
                self.c_channel.finalize()
                break

            if req[0] == channel.REQ_OPEN:
                MogamiLog.debug('** open **')
                self.open(req[1], req[2], req[3], *req[4])

            elif req[0] == channel.REQ_CREATE:
                MogamiLog.debug('** create **')
                self.create(req[1], req[2], req[3])

            elif req[0] == channel.REQ_READ:
                MogamiLog.debug('** read **')
                self.read(req[1], req[2])

            elif req[0] == channel.REQ_PREFETCH:
                MogamiLog.debug('** prefetch')
                self.prefetch(req[1], req[2])

            elif req[0] == channel.REQ_FLUSH:
                MogamiLog.debug('** flush')
                self.flush(req[1], req[2], req[3])

            elif req[0] == channel.REQ_RELEASE:
                MogamiLog.debug('** release **')
                self.release(req[1])
                self.c_channel.finalize()
                break

            elif req[0] == channel.REQ_TRUNCATE:
                MogamiLog.debug('** truncate')
                self.truncate(req[1], req[2])

            elif req[0] == channel.REQ_FTRUNCATE:
                MogamiLog.debug('** ftruncate')

            elif req[0] == channel.REQ_CLOSE:
                MogamiLog.debug("** quit **")
                self.c_channel.finalize()
                break

            elif req[0] == channel.REQ_FILEDEL:
                MogamiLog.debug("** filedel **")
                self.filedel(req[1])

            elif req[0] == channel.REQ_FILEREP:
                MogamiLog.debug("** filerep **")
                #print "** filerep parms = %s" % (str(req))
                self.filerep(req[1], req[2], req[3], req[4])

            elif req[0] == channel.REQ_RECVREP:
                MogamiLog.debug("** recvrep **")
                #print "** recvrep parms = %s" % (str(req))
                self.recvrep(req[1], req[2], req[3])

            elif req[0] == channel.REQ_FILEREQ:
                MogamiLog.debug("** filereq **")
                self.filereq(req[1])  # path

            elif req[0] == channel.REQ_FILEADD:
                MogamiLog.debug("** fileadd **")
                self.fileadd(req[1], req[2])  # mogami_path, data_path

            else:
                MogamiLog.debug('** this is unexpected header. break!')
                self.c_channel.finalize()
                break

    def truncate(self, path, length):
        MogamiLog.debug("path = %s. length = %d" % (path, length))
        try:
            f = open(path, "r+")
            f.truncate(length)
            f.close()
            ans = 0
        except Exception, e:
            MogamiLog.error("have an error (%s)" % (e))
            ans = e.errno
        self.c_channel.truncate_answer(ans)

    def open(self, mogamipath, datapath, flag, *mode):
        start_t = time.time()
        MogamiLog.debug("path = %s, flag = %s, mode = %s" %
                        (datapath, str(flag), str(mode)))
        flag = flag & ~os.O_EXCL
        try:
            fd = os.open(datapath, flag, *mode)
            ans = 0
            if conf.local_request is True:
                if self.filedict.checkfile(mogamipath) == None:
                    self.filedict.addfile(mogamipath, datapath)
        except Exception, e:
            fd = None
            ans = e.errno
        end_t = time.time()
        self.c_channel.open_answer(ans, fd, end_t - start_t)

    def read(self, fd, blnum):
        MogamiLog.debug("fd = %d, bl_num = %d" % (fd, blnum))

        sendbuf = ""
        try:
            os.lseek(fd, blnum * conf.blsize, os.SEEK_SET)
            buf = cStringIO.StringIO()
            readlen = 0
            while readlen < conf.blsize - 1:
                os.lseek(fd, blnum * conf.blsize + readlen, os.SEEK_SET)
                tmpbuf = os.read(fd, conf.blsize - readlen)
                if tmpbuf == '':   # end of file
                    break
                buf.write(tmpbuf)
                readlen += len(tmpbuf)
            sendbuf = buf.getvalue()
            ans = 0
        except Exception, e:
            MogamiLog.error("read have an error (%s)" % (e))
            ans = e.errno

        self.c_channel.data_send(ans, blnum, len(sendbuf), sendbuf)

    def prefetch(self, fd, blnum_list):
        for blnum in blnum_list:
            try:
                sendbuf = ""
                start_t = time.time()
                MogamiLog.debug("fd = %d, blnum = %d" % (fd, bl_num))
                os.lseek(fd, bl_num * conf.blsize, os.SEEK_SET)

                buf = cStringIO.StringIO()
                readlen = 0
                while readlen < conf.blsize - 1:
                    os.lseek(fd, bl_num * conf.blsize + readlen, os.SEEK_SET)
                    tmpbuf = os.read(fd, conf.blsize - readlen)
                    if tmpbuf == '':   # end of file
                        break
                    buf.write(tmpbuf)
                    readlen += len(tmpbuf)
                sendbuf = buf.getvalue()
                end_t = time.time()

                # send data read from file (only in case w/o error)
                self.c_channel.data_send(0, blnum, len(sendbuf), sendbuf)
            except Exception, e:
                MogamiLog.error("Prefetch Error!! with %d-th block" % (blnum))

    def flush(self, fd, listlen, datalen):
        MogamiLog.debug("fd=%d, listlen=%d, datalen=%d" %
                        (fd, listlen, datalen))

        (write_list, buf) = self.c_channel.flush_recv_data(listlen, datalen)
        if len(write_list) != 0:
            write_len = 0
            for wd in write_list:
                try:
                    ans = 0
                    os.lseek(fd, wd[0], os.SEEK_SET)
                    ret = os.write(fd, buf[write_len:write_len + wd[1]])
                    write_len += ret
                    if ret != wd[1]:
                        MogamiLog.error("write length error !!")
                        break
                    MogamiLog.debug("write from offset %d (result %d)" %
                                    (wd[0], ret))
                except OSError, e:
                    ans = e.errno
                    break
                #except Exception, e:
                #    ans = -1
                #    break

            self.c_channel.flush_answer(ans, write_len)

    def release(self, fd):
        try:
            os.fsync(fd)
            st = os.fstat(fd)
            os.close(fd)
            size = st.st_size
            ans = 0
        except Exception, e:
            ans = e.errno
            size = 0

        self.c_channel.release_answer(ans, size)

    # MogamiSystem APIs
    def filedel(self, file_list):
        try:
            for file_name in file_list:
                os.unlink(file_name)
            ans = 0
        except Exception, e:
            ans = e.errno

        self.c_channel.filedel_answer(ans)

    def filerep(self, mogami_path, org_path, dest, dest_path):
        to_channel = channel.MogamiChanneltoData(dest)
        
        f = open(org_path, 'r')
        f_size = os.fstat(f.fileno()).st_size
        to_channel.recvrep_req(mogami_path, dest_path, f_size)
        send_size = 0
        
        while send_size < f_size:
            buf = f.read(conf.blsize)
            to_channel.sock.sendall(buf)
            send_size += len(buf)
        assert (send_size == f_size)

        MogamiLog.debug("finish send data of file")
        ans = to_channel.recvrep_getanswer()
        #print "finished replication (ans: %d)" % (ans)
        if ans == 0:
            self.c_channel.filerep_answer(ans, f_size)
        to_channel.finalize()

    def recvrep(self, mogami_path, data_path, f_size):
        try:
            f = open(data_path, 'w+')
            write_size = 0
            MogamiLog.debug("replication will be created! size = %d" % (f_size))
            
            while write_size < f_size:
                if (f_size - write_size) < conf.blsize:
                    buf = self.c_channel.recvall(f_size - write_size)
                else:
                    buf = self.c_channel.recvall(conf.blsize)
                f.write(buf)
                write_size += len(buf)
            f.close()
            ans = 0
            if conf.local_request is True:
                self.filedict.addfile(mogami_path, data_path)
        except Exception:
            ans = -1
        self.c_channel.recvrep_answer(ans)

    def filereq(self, path):
        try:
            local_path = self.filedict.checkfile(path)
            if local_path != None:
                fsize = os.path.getsize(local_path)
                ans = 0
            else:
                ans = -1
                fsize = 0
        except KeyError:
            ans = -1
            local_path = None
            fsize = 0
        self.c_channel.filereq_answer(ans, local_path, fsize)

    def fileadd(self, mogami_path, data_path):
        self.filedict.addfile(mogami_path, data_path)

class MogamiData(object):
    """This is the class of mogami's data server
    """
    def __init__(self, metaaddr, rootpath):
        """This is the function of MogamiMeta's init.

        @param metaaddr ip address or hostname of metadata server
        @param rootpath path of directory to store data into
        """
        # basic information
        self.metaaddr = metaaddr
        self.rootpath = os.path.abspath(rootpath)
        self.filedict = MogamiLocalFileDict()

        # check directory for data files
        assert os.access(self.rootpath, os.R_OK and os.W_OK and os.X_OK)

        # Initialization of Log.
        MogamiLog.init("data", conf.data_loglevel)
        MogamiLog.info("Start initialization...")
        MogamiLog.debug("rootpath = " + self.rootpath)

        # At first, connect to metadata server and send request to attend.
        self.m_channel = channel.MogamiChanneltoMeta()
        self.m_channel.connect(self.metaaddr)
        MogamiLog.debug("Success in creating connection to metadata server")
        self.m_channel.dataadd_req(self.rootpath)

        MogamiLog.debug("Init complete!!")

    def run(self, ):
        # create a socket to listen and accept
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.lsock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.lsock.bind(("0.0.0.0", conf.dataport))
        self.lsock.listen(10)
        MogamiLog.debug("Listening on the port " + str(conf.dataport))

        # create a thread to collect dead daemon threads
        daemons_list = []
        collector_thread = daemons.MogamiThreadCollector(daemons_list)
        collector_thread.start()
        threads_count = 0

        while True:
            # connected from client
            (csock, address) = self.lsock.accept()
            MogamiLog.debug("accept connnect from " + str(address[0]))


            client_channel = channel.MogamiChannelforData()
            client_channel.set_socket(csock)

            datad = MogamiDataHandler(client_channel, self.rootpath,
                                      self.filedict)
            datad.name = "D%d" % (threads_count)
            threads_count += 1
            datad.start()
            daemons_list.append(datad)
            MogamiLog.debug("Created thread name = %s (%d-th threads)" %
                            (datad.getName(), threads_count))

    def finalize(self, ):
        if self.m_channel == None:
            self.m_channel = channel.MogamiChanneltoMeta()
            self.m_channel.connect(self.metaaddr, conf.metaport)
        self.m_channel.datadel_req()
        self.m_channel.finalize()


def main(meta_addr, dir_path):
    data = MogamiData(meta_addr, dir_path)
    atexit.register(data.finalize)
    data.run()


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
