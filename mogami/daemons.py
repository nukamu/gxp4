#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement

import threading
import select
import time

import conf
import channel
from system import MogamiLog


class MogamiDaemons(threading.Thread):
    """Base class for all daemons.
    All daemons in Mogami should be derived from this class.
    """
    def __init__(self, ):
        threading.Thread.__init__(self)
        self.setDaemon(True)


class MogamiThreadCollector(MogamiDaemons):
    """Collect dead threads in arguments.
    
    @param daemons thread list to collect: alive and dead threads are included
    """
    sleep_time = 3.0
    def __init__(self, daemons):
        MogamiDaemons.__init__(self)
        self.daemons = daemons   # list of target daemons

    def run(self, ):
        while True:
            daemons_alive = threading.enumerate()
            for d in self.daemons:
                if d not in daemons_alive:
                    d.join()
                    MogamiLog.debug("** join thread **")
                    self.daemons.remove(d)
            time.sleep(self.sleep_time)


class MogamiRequestHandler(MogamiDaemons):
    """Request handler classes on metadata and data server should be
    derived from this class. (data.MogamiDataHandler & meta.MogamiMetaHandler)
    """
    def __init__(self, ):
        MogamiDaemons.__init__(self)
        self.func_dict = {}
        # And in derived class's init,
        # the following member valiables are defined at least: 
        #    self.c_channel

    def regist_handler(self, req, funcname):
        """Regist a function to handle a request.

        @param req type of request
        @param funcname name of function to handle request
        """
        self.func_dict[req] = funcname
        
    def run(self, ):
        while True:
            req = self.c_channel.recv_request()
            if req == None:  # failed to receive data
                MogamiLog.debug("Connection closed by failing recv data")
                self.c_channel.finalize()
                break
            elif req[0] == channel.REQ_CLOSE:  # close request
                MogamiLog.debug("Connection closed by close request")
                self.c_channel.finalize()
                break

            if req[0] in self.func_dict:
                MogamiLog.debug('** %s **' % (self.func_dict[req[0]]))
                method = getattr(self, self.func_dict[req[0]])
                req_args = list(req)
                req_args.pop(0)
                method(*req_args)

            else:  # doesn't match any expected request
                MogamiLog.error('** This is unexpected header **')
                self.c_channel.finalize()
                break


class MogamiPrefetchThread(MogamiDaemons):
    """Class for the thread to recv data.
    """
    def __init__(self, mogami_file):
        MogamiDaemons.__init__(self)
        self.mogami_file = mogami_file
        self.p_channel = mogami_file.p_channel
        MogamiLog.debug("** [prefetch thread] init OK")

    def run(self, ):
        pre_num_change = False
        while True:
            readable = select.select(
                [self.p_channel.sock.fileno()], [], [], 0)
            pre_num_change = (len(readable[0]) == 0)
            if recv_prefetch_data() == False:  # succeeded or not
                pass

    def recv_prefetch_data(self, ):
        time_list = []

        select.select([self.p_channel.sock.fileno()], [], [])

        header = self.p_channel.recv_msg()
        if header == None:
            MogamiLog.debug("break prefetch thread's loop")
            self.mogami_file.r_data = None
            return

        errno = header[0]
        blnum = header[1]
        size = header[2]
        
        if size == 0:
            with self.mogami_file.r_buflock:
                self.mogami_file.r_data[blnum].state = 2
                self.mogami_file.r_data[blnum].buf = ""
            return True  # succeeded

        select.select([self.p_channel.sock.fileno()], [], [])    
        (buf, recv_time) = self.p_channel.recvall_with_time(size)

        if conf.prefetch == True:
            time_list.append(recv_time)
            eval_time = 0
            if len(time_list) > 5:
                time_list.pop(0)
            for t in time_list:
                eval_time += t
                eval_time /= len(time_list)
            if len(buf) != size:
                MogamiLog.debug("break prefetch thread's loop")
                self.mogami_file.r_data = None
                return
            if pre_num_change == True:
                recv_size = size / float(1024) / float(1024)
                self.mogami_file.prenum = int(recv_size / eval_time *
                                              self.mogami_file.rtt)
                self.mogami_file.prenum += 1
                MogamiLog.debug("prenum is changed to %d" %
                                (self.mogami_file.prenum))
                MogamiLog.debug("time = %f, eval_time = %f" %
                                (recv_time, eval_time))

                MogamiLog.debug("prefetch recv %d byte bnum %d" %
                                (len(buf), blnum))

        if self.mogami_file.r_data != None:
            with self.mogami_file.r_buflock:
                self.mogami_file.r_data[blnum].state = 2
                self.mogami_file.r_data[blnum].buf = buf
