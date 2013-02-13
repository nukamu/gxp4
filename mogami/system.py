#! /usr/bin/env python
#-*- coding: utf-8 -*-

from __future__ import with_statement

from fuse import Fuse
import fuse
fuse.fuse_python_api = (0, 2)

import conf
import logging
import os.path
import sys


class Singleton(type):
    """Singleton Class implementation from
    http://code.activestate.com/recipes/412551/
    """

    def __init__(self, *args):
        type.__init__(self, *args)
        self._instance = None

    def __call__(self, *args):
        if self._instance is None :
            self._instance = type.__call__(self, *args)
        return self._instance


class MogamiLog(object):
    """Mogami Logger Class.
    """
    __metaclass__ = Singleton
    
    # Type of Component
    TYPE_FS = 0
    TYPE_META = 1
    TYPE_DATA = 2
    TYPE_SCHEDULER = 3

    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL
    
    def __init__(self, *args):
        """
        >>> i1 = MogamiLog()
        >>> i2 = MogamiLog()
        >>> assert(i1 == i2)
        """
        pass

    @staticmethod
    def init(log_type, output_level):
        """Initialize logger.

        @param log_type
        @param output_level
        >>> MogamiLog.init("meta", MogamiLog.DEBUG)
        """
        instance = MogamiLog()

        logdir = conf.log_dir
        if log_type == self.TYPE_FS:
            instance.logfile = os.path.join(logdir, "mogami.log")
        elif log_type == self.TYPE_META:
            instance.logfile = os.path.join(logdir, "meta.log")
        elif log_type == self.TYPE_DATA:
            instance.logfile = os.path.join(logdir, "data.log")
        elif log_type == self.TYPE_SCHEDULER:
            instance.logfile = os.path.join(logdir, "scheduler.log")
        else:
            raise
        if os.access(logdir, os.W_OK) == False:
            sys.exit("""** Directory for log is permitted to write. **
Please confirm the directory "%s".""" % (logdir))
        logging.basicConfig(filename=instance.logfile, 
                            level=output_level,
                            format='[%(asctime)s] %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S')
        logging.info("Logging Start")    

    @staticmethod
    def debug(msg):
        logging.debug(msg)
    
    @staticmethod
    def info(msg):
        logging.info(msg)
    
    @staticmethod
    def warning(msg):
        logging.warning(msg)

    @staticmethod
    def error(msg):
        logging.error(msg)

    @staticmethod
    def critical(msg):
        logging.critical(msg)

def usagestr():
    """Usage string.
    """
    return ""+ fuse.Fuse.fusage

class MogamiError(Exception):
    def __init__(self, typeno, ):
        pass

    def __str__(self, ):
        pass


if __name__ == "__main__":
    import doctest
    doctest.testmod()
