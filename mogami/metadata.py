#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import os, os.path
import errno
import conf

class MogamiMetaFS(object):
    """Class for managing metadata on local file system (like ext3).

    supposed to be used in metadata server's daemon.
    """
    def __init__(self, dir_path):
        # rootpath must be absolute path
        # tail string must not '/'
        self.rootpath = dir_path

        # check directory for data files
        if os.access(self.rootpath, os.R_OK and os.W_OK and os.X_OK) == False:
            sys.exit("%s is not permitted to use. " % (rootpath, ))
            

    def _get_metadata(self, path):
        """read metadata from file

        this function must return tuple with three elements (None with error)
        @param path path of file to get metadata
        """
        try:
            with open(self.rootpath + path, 'r') as f:
                (dest, data_path, fsize) = f.read().rsplit(',')
                fsize = int(fsize)
        except ValueError, e:
            e = OSError('metadata is crashed (file: %s)' % (data_path))
            e.errno = 2
            raise e
        return dest, data_path, fsize

    def open(self, path, flag, mode):
        
        # once check if the file can be opened with flag
        # if cannot be opened, raise with errno
        if mode:
            fd = os.open(self.rootpath + path, os.O_RDWR, mode[0])
        else:
            fd = os.open(self.rootpath + path, os.O_RDWR)
        (dest, data_path, fsize) = os.read(fd, conf.bufsize).rsplit(',')
        fsize = int(fsize)
        os.close(fd)
        return dest, data_path, fsize

    def create(self, path, flag, mode, dest, data_path):

        # once check if the file can be opened with flag
        # if cannot be opened, raise with errno
        if mode:
            fd = os.open(self.rootpath + path, os.O_RDWR | os.O_CREAT, mode[0])
        else:
            fd = os.open(self.rootpath + path, os.O_RDWR | os.O_CREAT)
        os.write(fd, "%s,%s,0" % (dest, data_path))
        os.close(fd)

    def release(self, path, fsize):
        with open(self.rootpath + path, 'r+') as f:
            (dest, path, org_size) = f.read().rsplit(',')
            org_size = int(org_size)
            if fsize != org_size:
                f.truncate(0)
                f.seek(0)
                f.write("%s,%s,%d" % (dest, path, fsize))

    def access(self, path, mode):
        return os.access(self.rootpath + path, mode)

    def getattr(self, path):
        """wrapper of os.lstat and might change the file size.

        @path path of file to get attributes
        @return tuple of st(result of stat) and fsize
        """
        st = os.lstat(self.rootpath + path)
        if os.path.isfile(self.rootpath + path):
            fsize = self._get_metadata(path)[2]
        else:
            fsize = -1
        return st, fsize

    def readdir(self, path):
        return os.listdir(self.rootpath + path)

    def mkdir(self, path):
        return os.mkdir(self.rootpath + path, mode)

    def rmdir(self, path):
        return os.rmdir(self.rootpath + path)
    
    def unlink(self, path):
        return os.unlink(self.rootpath + path)
            
    def rename(self, oldpath, newpath):
        return os.rename(self.rootpath + oldpath, self.rootpath + newpath)

    def chmod(self, path, mode):
        return os.chmod(self.rootpath + path, mode)

    def chown(self, path, uid, gid):
        return os.chown(self.rootpath + path, uid, gid)

    def symlink(self, frompath, topath):
        return os.symlink(frompath, self.rootpath + topath)
    
    def readlink(self, path):
        return os.readlink(self.rootpath + path)

    def truncate(self, path, size):
        with open(self.rootpath + path, 'r+') as f:
            (dest, data_path, org_size) = f.read().rsplit(',')
            org_size = int(org_size)
            f.truncate(0)
            f.seek(0)
            f.write("%s,%s,%d" % (dest, data_path, size))
        return dest, data_path

    def utime(self, path, times):
        return os.utime(self.rootpath + path, times)

class MogamiMetaDB(object):
    """Class for managing metadata in db (using sqlite3).

    supposed to be used in metadata server's daemon.
    """
    def __init__(self, path):
        self.db_file = "mogami_meta.db"
        self.db_path = os.path.join(path, self.db_file)
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.db_conn = sqlite3.connect(self.db_path)
        self.db_cur = self.db_conn.cursor()
    
        # create a table for files
        self.db_cur.execute("""
        CREATE TABLE content (
        path TEXT PRIMARY KEY,
        par_dir TEXT,
        mode INT,
        uid INT,
        gid INT,
        nlink INT,
        size INT,
        atime INT,
        mtime INT,
        ctime INT,
        dest TEXT,
        dest_path TEXT
        )
        """)
        
    def _set_file(self, path, mode, uid, gid, ulink, size, atime, mtime,
                  ctime, dest, dest_path):
        r = self.db_cur.execute("SELECT * FROM content WHERE path = ?;", (path,)).fetchone()
        if r:
            return errno.EEXIST
        self.db_cur.execute("""
        INSERT INTO content (
        path, mode, uid, gid, nlink, size, atime, mtime, ctime, dest, dest_path) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (path, mode, uid, gid, nlink, size,
              atime, mtime, ctime, dest, dest_path))
        self.db_conn.commit()
        return 0

    def _rm_file(path):
        r = self.db_cur.execute("", (path))

    def _set_dir(self, path, st):
        """register metadata of a file to DB

        @param path path of the directory
        @param st return value of os.lstat(path)
        @return if success 0, error number with error
        """
        self.db_cur.execute("""
            INSERT INTO dirs (
            path, mode, uid, gid, ulink, size, atime, mtime, ctime,""")

    def _rm_dir(self, path):
        r = self.db_cur.execute("", (path))

    def dump_all(self, ):
        """print information of all files
        """
        self.db_cur.execute("""
        SELECT * FROM content""")
        l = self.db_cur.fetchall()
        for tmp in l:
            for tmp2 in tmp:
                print tmp2,
            print ""
    
    def _return_st(self, path):
        """return metadata statement of a file

        this function can be used as os.lstat
        return None, if file doesn't exist

        @param path file path
        """
        self.db_cur.execute("""
        SELECT mode, uid, gid, nlink, size, atime, mtime, ctime 
        FROM content WHERE path = ?""", (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            return None  # the file doesn't exist
        st_org = l[0]
        st = tips.MogamiStat()
        i = 0
        for attr in st.mogami_attrs:
            setattr(st, attr, st_org[i])
            i += 1
        return st

    def _return_dest(self, path):
        """will return which data server has required file.

        @param path path of required file
        @return [ip_of_data_server, file_path_on_the_data_server]
        """
        self.db_cur.execute("""
        SELECT dest, dest_path 
        FROM content WHERE path = ?""", (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            return None
        ret = [str(l[0][0]), str(l[0][1])]
        return ret

    def access(self, path, mode):
        """
        This function should return either True or False.
        @param path path of file to look for
        @param mode one of os.F_OK, os.R_OK, os.W_OK and os.X_OK
        """
        self.db_cur.execute("""
        SELECT mode, uid, gid FROM content WHERE path = ?""", (path))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            return False
        if mode == F_OK:
            return True

        # check file mode and access mode
        pass

    def getattr(self, path):
        self.db_cur.execute("""
        SELECT mode, uid, gid, nlink, size, atime, mtime, ctime 
        FROM content WHERE path = ?""", (path))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)
        st_org = l[0]
        st = tips.MogamiStat()
        i = 0
        for attr in st.mogami_attrs:
            setattr(st, attr, st_org[i])
            i += 1
        return st

    def listdir(self, path):
        self.db_cur.execute("""
        SELECT path FROM content WHERE par_dir = ?""", (path))

    def mkdir(self, path):
        self.db_cur.execute(""" 
        """)

    def rmdir(self, path):
        self.db_cur.execute("""
        SELECT mode, uid, gid, nlink, size, atime, mtime, ctime 
        FROM content WHERE path = ?""", (path))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)

    
    def unlink(self, path):
        self.db_cur.execute("""
        SELECT mode, uid, gid, nlink, size, atime, mtime, ctime 
        FROM content WHERE path = ?""", (path))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)
            
    def rename(self, oldpath, newpath):
        ## extract a record and update (path)
        self.db_cur.execute("""
        SELECT mode, uid, gid, nlink, size, atime, mtime, ctime 
        FROM content WHERE path = ?""", (path))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)

    def chmod(self, path, mode):
        ## extract a record and update (mode)
        self.db_cur.execute("""
        SELECT mode, uid, gid, nlink, size, atime, mtime, ctime 
        FROM content WHERE path = ?""", (path))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)

    def chown(self, path, uid, gid):
        ## extract a record and update (uid, gid)
        self.db_cur.execute("""
        SELECT mode, uid, gid, nlink, size, atime, mtime, ctime 
        FROM content WHERE path = ?""", (path))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)

    def truncate(self, path, length):
        ## extract a record and update (size)
        self.db_cur.execute("""
        SELECT mode, uid, gid, nlink, size, atime, mtime, ctime 
        FROM content WHERE path = ?""", (path))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)


    def utime(self, path, times):
        """
        @path file path to modify times
        @param times tuple of (atime, mtime)
        """
        pass

    def _check_permission(self, mode):
        """
        @return 
        """
        pass

    def _raise_with_error(self, num):
        e = os.error(os.strerror(num))
        e.errno = num
        raise e

if __name__ == '__main__':
    import doctest
    doctest.testmod()
