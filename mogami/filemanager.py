#! /usr/bin/env python
#-*- coding: utf-8 -*-

from __future__ import with_statement

import os
import sys
import threading
import cStringIO
import time
import os.path
import Queue

from fuse import Fuse
import fuse
fuse.fuse_python_api = (0, 2)


import conf
import channel
from system import MogamiLog


class MogamiAccessPattern(object):
    """Access pattern repository for a file
    """
    read = 0
    write = 1

    def __init__(self, path, cmds, pid):
        """initializer.

        @param path file path
        @param cmds cmd args of process which open the file
        @param pid pid of process which open the file
        """
        self.path = path
        self.cmd_args = cmds
        self.pid = pid
        self.queue = Queue.Queue()

        self.parents_list = self.get_parents()

    def insert_data(self, ops, offset, length):
        self.queue.put((ops, offset, length))
            
    def mk_form_data(self, ):
        read_data = []
        write_data = []
        while True:
            try:
                item = self.queue.get_nowait()
                if item[0] == self.read:
                    read_data.append((item[1], item[2]))
                else:
                    write_data.append((item[1], item[2]))
            except Queue.Empty:
                break
        
        read_data.sort()
        write_data.sort()

        tmp_list = []
        next_expected_offset = -1
        for data in read_data:
            if data[0] == next_expected_offset:
                former_data = tmp_list.pop()
                tmp_list.append((former_data[0], former_data[1] + data[1]))
            else:
                tmp_list.append(data)
            next_expected_offset = data[0] + data[1]
        read_data = tmp_list

        tmp_list = []
        next_expected_offset = -1
        for data in write_data:
            if data[0] == next_expected_offset:
                former_data = tmp_list.pop()
                tmp_list.append((former_data[0], former_data[1] + data[1]))
            else:
                tmp_list.append(data)
            next_expected_offset = data[0] + data[1]
        write_data = tmp_list

        return (read_data, write_data)

    def get_parents(self, ):
        """get pids of parent processes
        """
        ret_list = []  # pids list (order shows relation)
        
        child_pid = self.pid

        count = 0
        while True:
            try:
                with open("/proc/%s/stat" % (str(child_pid)), "r") as f:
                    buf = f.read()
                ppid = int(buf.split(' ')[3])
                if ppid == 1:
                    break
                ret_list.append(ppid)
                child_pid = ppid
            except Exception, e:
                break
            count += 1
            if count > 10:  # tentatively
                break
        return ret_list

class MogamiStat(fuse.Stat):
    attrs = ("st_mode", "st_ino", "st_dev", "st_uid", "st_gid", "st_nlink",
             "st_size", "st_atime", "st_mtime", "st_ctime")
    mogami_attrs = ("st_mode", "st_uid", "st_gid", "st_nlink",
                    "st_size", "st_atime", "st_mtime", "st_ctime")

    def __init__(self, ):
        for attr in self.attrs:
            try:
                setattr(self, attr, 0)
            except AttributeError, e:
                print e

    def __repr__(self):
        s = ", ".join("%s=%d" % (attr, getattr(self, "_" + attr))
                      for attr in self.mogami_attrs)
        return "<MogamiStat %s>" % (s,)

    def load(self, st_dict):
        for attr in self.attrs:
            try:
                setattr(self, attr, st_dict[attr])
            except AttributeError, e:
                print e

    # TODO: Deprecated
    def chsize(self, size):
        self.st_size = size

class MogamiBlock(object):
    """Class of a object of a block
    """

    def __init__(self, ):
        """state -> Data are..
        0: not exist
        1: requesting
        2: exist
        """
        self.state = 0

        # block data
        self.buf = ""

class MogamiFileRepository(object):
    """Class for a repository of file data.
    
    Client has an instance of this class and manage file data using it.
    ** important **  this class is not used now!
    """
    def __init__(self, ):
        pass


    def add_file(self, ):
        pass

class MogamiFile(object):
    """Class for a object of a file
    """
    def __init__(self, fsize):
        self.fsize = fsize


class MogamiRamFile(object):
    """Class for managing a content of file on memory.

    This class is not implemented completely.
    """

    def __init__(self, size):
        self.buf = cStringIO.StringIO()
        self.block_num = 0
        self.errno = 0

    def read(self, length):
        self.buf.read(length)

    def write(self, buf):
        self.buf.write(buf)


class MogamiRemoteFile(MogamiFile):
    """Class for a file located at remote node.
    """
    def __init__(self, fsize, dest, mogami_path, data_path, flag, *mode):
        """initializer. This method is called by open().

        @param fsize file size
        @param dest ip of node which have data contents
        @param data_path path of file contents at data server
        @param flag file open flag
        @param mode file open mode
        """
        # initialize some information
        MogamiFile.__init__(self, fsize)
        self.remote = True
        self.dest = dest
        self.data_path = data_path
        self.flag = flag
        self.mode = mode
        self.mogami_path = mogami_path
        
        # calculation of the number of blocks
        self.blnum = self.fsize / conf.blsize
        if self.fsize % conf.blsize != 0:
            self.blnum += 1

        # initialization of read buffer
        self.r_data = tuple([MogamiBlock() for i in range(self.blnum + 1)])
        self.r_buflock = threading.Lock()
        MogamiLog.debug("create r_data 0-%d block" % (len(self.r_data)-1))

        # initialization of write buffer
        self.w_list = []
        self.w_data = cStringIO.StringIO()
        self.w_buflock = threading.Lock()
        self.w_len = 0
        # for buffer of dirty data
        self.dirty_dict = {}

        # information for prefetching
        self.prenum = 1

        
    def create_connections(self, channel_repository=None):
        """Create connections to data server which has file contents.

        In this function, send request for open to data server.
        (and calculate RTT)
        """
        self.d_channel = channel.MogamiChanneltoData(self.dest)

        # send a request to data server for open
        start_t = time.time()
        (ans, self.datafd, open_t) = self.d_channel.open_req(
            self.mogami_path, self.data_path, self.flag, *self.mode)
        end_t = time.time()
        if ans != 0:  # failed...with errno
            self.finalize()
            return ans

        # on success
        self.rtt = end_t - start_t - open_t
        # must be 0
        return ans


    def prepare_for_prefetch(self, ):
        """
        """
        pass

    def finalize(self, ):
        self.r_data = None
        self.d_channel.finalize()

    def read(self, length, offset):
        requestl = self.cal_bl(offset, length)

        # prepare buffer to return
        ret_str = cStringIO.StringIO()
        MogamiLog.debug("requestl = %s, with offset: %d, length: %d" %
                        (str(requestl), offset, length))
        for req in requestl:
            reqbl = req[0]
            buf = ""     # buffer for block[reqbl]
            last_readbl = reqbl

            if self.r_data[reqbl].state == 2:
                buf = self.r_data[reqbl].buf
            elif self.r_data[reqbl].state == 1:
                MogamiLog.debug("Waiting recv data %d block" % reqbl)
                while self.r_data[reqbl].state == 1:
                    time.sleep(0.01)
                buf = self.r_data[reqbl].buf
            else:
                self.request_block(reqbl)
                with self.r_buflock:
                    buf = self.r_data[reqbl].buf

            # check dirty data (and may replace data)
            with self.w_buflock:
                if reqbl in self.dirty_dict:
                    dirty_data_list = self.dirty_dict[reqbl]
                    for dirty_data in dirty_data_list:
                        # remember the seek pointer of write buffer
                        seek_point = self.w_data.tell()
                        self.w_data.seek(dirty_data[2])
                        dirty_str = self.w_data.read(
                            dirty_data[1] - dirty_data[0])
                        self.w_data.seek(seek_point)
                        tmp_buf = buf[dirty_data[1]:]
                        buf = buf[0:dirty_data[0]] + dirty_str + tmp_buf

            # write strings to return and if reach EOF, break
            ret_str.write(buf[req[1]:req[2]])
            if len(buf) < conf.blsize:
                break    # end of file

        return ret_str.getvalue()

    def write(self, buf, offset):
        # recalculation of the number of blocks
        prev_blnum = self.blnum
        self.blnum = self.fsize / conf.blsize
        if self.fsize % conf.blsize != 0:
            self.blnum += 1
        if prev_blnum < self.blnum:
            self.r_data += tuple([MogamiBlock() for i in
                                  range(self.blnum - prev_blnum)])

        tmp = (offset, len(buf))
        prev_writelen = self.w_len
        with self.w_buflock:
            self.w_list.append(tmp)
            self.w_data.write(buf)
            self.w_len += len(buf)

        reqs = self.cal_bl(offset, len(buf))
        dirty_write = 0
        for req in reqs:
            if req[0] in self.dirty_dict:
                self.dirty_dict[req[0]].append((req[1], req[2],
                                                prev_writelen))
            else:
                self.dirty_dict[req[0]] = [(req[1], req[2],
                                            prev_writelen), ]
        if self.w_len > conf.writelen_max:
            self.flush()
        return len(buf)

    def flush(self, ):
        if self.w_len == 0:
            return 0
        with self.w_buflock:
            send_data = self.w_data.getvalue()

            MogamiLog.debug("flush: fd=%d, listlen=%d, datalen=%d" %
                            (self.datafd, len(self.w_list), len(send_data)))
            (ans, w_len) = self.d_channel.flush_req(self.datafd,
                                                    self.w_list, send_data)
            self.w_len = 0
            self.w_list = []
            self.w_data = cStringIO.StringIO()
            self.dirty_dict = {}
        return ans

    def fsync(self, isfsyncfile):
        if self.w_len == 0:
            pass
        else:
            self.flush()

    def release(self, flags):
        if self.w_len != 0:
            self.flush()
        (ans, fsize) = self.d_channel.release_req(self.datafd)
        self.finalize()
        return fsize

    def request_block(self, blnum):
        """send request of block data.
        
        @param blnum the number of block to require
        """
        MogamiLog.debug("** read %d block" % (blnum))
        MogamiLog.debug("request to data server %d block" % (blnum))

        # change status of the block (to requiring)
        with self.r_buflock:
            self.r_data[blnum].state = 1
       
        bldata = self.d_channel.read_req(self.datafd, blnum)

        if bldata == None:
            self.r_data = None       

        with self.r_buflock:
            self.r_data[blnum].state = 2
            self.r_data[blnum].buf = bldata

    def request_prefetch(self, blnum_list):
        """send request of prefetch.

        @param blnum_list the list of block numbers to prefetch
        """
        MogamiLog.debug("** prefetch ** required blocks = %s" % 
                        (str(blnum_list)))
        # send request to data server
        self.p_channel.prefetch_req(self.datafd, blnum_list)

        with self.r_buflock:
            for blnum in blnum_list:
                self.r_data[blnum].state = 1

    def cal_bl(self, offset, size):
        """This function is used for calculation of request block number.

        return list which show required block number.
        @param offset offset of read request
        """
        blbegin = offset / conf.blsize
        blend = (offset + size - 1) / conf.blsize + 1
        blnum = range(blbegin, blend)

        blfrom = [offset % conf.blsize, ]
        blfrom.extend([0 for i in range(len(blnum) - 1)])

        blto = [conf.blsize for i in range(len(blnum) - 1)]
        least = (offset + size) % conf.blsize

        if least == 0:
            least = conf.blsize
        blto.append(least)

        return zip(blnum, blfrom, blto)


class MogamiLocalFile(MogamiFile):
    """Class for a file located at local storage
    """
    def __init__(self, fsize, local_ch, mogami_path, data_path, flag, *mode):
        MogamiFile.__init__(self, fsize)
        self.remote = False
        
        # make information for open
        md = {os.O_RDONLY: 'r', os.O_WRONLY: 'w', os.O_RDWR: 'w+'}
        m = md[flag & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]
        if flag | os.O_APPEND:
            m = m.replace('w', 'a', 1)

        created = True
        
        if os.access(data_path, os.F_OK) == True:
            created = False

        # open the file actually
        self.file = os.fdopen(os.open(data_path, flag, *mode), m)
        self.lock = threading.Lock()

        if conf.local_request is True:
            # add to data server's metadata cache
            if created == True:
                if local_ch.connected == False:
                    local_ch.connect('127.0.0.1')  # to local
                local_ch.fileadd_req(mogami_path, data_path)
                

    def read(self, length, offset):
        with self.lock:
            # read data from local file
            self.file.seek(offset)
            return self.file.read(length)

    def write(self, buf, offset):
        with self.lock:
            # write data to local file
            self.file.seek(offset)
            self.file.write(buf)
        return len(buf)

    def flush(self, ):
        if 'w' in self.file.mode or 'a' in self.file.mode:
            self.file.flush()
        return 0

    def fsync(self, isfsyncfile):
        if 'w' in self.file.mode or 'a' in self.file.mode:
            self.file.flush()
        if isfsyncfile and hasattr(os, 'fdatasync'):
            os.fdatasync(self.file.fileno())
        else:
            os.fsync(self.file.fileno())
        return 0

    def release(self, flags):
        st = os.fstat(self.file.fileno())
        filesize = st.st_size
        self.file.close()
        return st.st_size

    def finalize(self, ):
        self.file.close()


class MogamiFileAccessPattern(object):
    """This is the class to manage the access pattern of a file.
    now not be used..
    """
    MOD_SEQ = 0
    MOD_STRIDE = 1
    MOD_RAND = 2

    def __init__(self, ):
        """
        >>> access_pattern = AccessPattern() 
        """
        self.mode = AccessPattern.MOD_SEQ
        self.stride_read_size = 0
        self.stride_seek_size = 0
        self.lock = threading.Lock()
        self.last_read = 0
        self.last_bl = 0
        self.last_seq_read_start = 0

    def change_mode(self, mode, args=[]):
        """
        >>> access_pattern.change_mode(AccessPattern.MOD_STRIDE, (16, 16))
        """
        with self.lock:
            self.mode = mode
            if len(args) == 0:
                return
            self.stride_read_size = args[0]
            self.stride_seek_size = args[1]

    def tell_mode(self, ):
        return self.mode

    def check_mode(self, offset, size):
        with self.lock:
            if self.mode == AccessPattern.MOD_SEQ:
                if offset == self.last_read:
                    #print "[tips] mode sequential"
                    return
                elif offset < self.last_read:
                    #print "[tips] mode rand"
                    self.mode = AccessPattern.MOD_RAND
                else:
                    #print "[tips] mode stride"
                    self.mode = AccessPattern.MOD_STRIDE
                    self.stride_read_size = self.last_read - self.last_seq_read_start
                    self.stride_seek_size = offset - self.last_read
            # case of stride accessing
            elif self.mode == AccessPattern.MOD_STRIDE:
                if offset == self.last_read:
                    # 
                    if offset + size - self.last_seq_read_start > self.stride_read_size:
                        self.mode = AccessPattern.MOD_SEQ
                    else:
                        return
                else:
                    if (self.last_read - self.last_seq_read_start == self.stride_read_size) and (offset - self.last_read == self.stride_seek_size) and (size <= self.stride_read_size):
                        #print "[tips] mode stride"
                        return
                    else:
                        #print "[tips] mode rand"
                        self.mode = AccessPattern.MOD_RAND
                    
            else:
                if offset == self.last_read:
                    #print "[tips] mode sequential"
                    self.mode = AccessPattern.MOD_SEQ


    def change_info(self, offset, res_size, last_bl):
        if self.last_read != offset:
            self.last_seq_read_start = offset
        self.last_read = offset + res_size
        self.last_bl = last_bl

    def return_need_block(self, last_read, bl_num, max_bl_num):
        """
        """
        ret_block_list = []
        start_t = time.time()
        start_bl = last_read / conf.blsize
        if last_read % conf.blsize == 0 and last_read != 0:
            start_bl -= 1

        with self.lock:
            if self.mode == AccessPattern.MOD_SEQ:
                if start_bl + 1 + bl_num > max_bl_num:
                    ret_block_list = range(start_bl + 1, max_bl_num)
                else:
                    ret_block_list = range(start_bl + 1, start_bl + 1 + bl_num)
                return ret_block_list
            elif self.mode == AccessPattern.MOD_STRIDE:
                next_offset = last_read + self.stride_seek_size
                if self.stride_read_size + self.stride_seek_size < conf.blsize:
                    if start_bl + 1 + bl_num > max_bl_num:
                        ret_block_list = range(start_bl + 1, max_bl_num)
                    else:
                        ret_block_list = range(start_bl + 1, start_bl + 1 + bl_num)
                    return ret_block_list
                while True:
                    req_list = self.cal_bl_list(next_offset, self.stride_read_size)
                    if req_list[0] > max_bl_num:
                        break
                    for req_bl in req_list:
                        if req_bl not in ret_block_list and start_bl < req_bl:
                            ret_block_list.append(req_bl)
                    if len(ret_block_list) >= bl_num or ret_block_list[-1] >= max_bl_num:
                        break
                    next_offset += self.stride_read_size + self.stride_seek_size
        return ret_block_list[:bl_num]

    def cal_bl_list(self, offset, size):
        blbegin = offset / conf.blsize
        blend = (offset + size - 1) / conf.blsize + 1
        return range(blbegin, blend)

