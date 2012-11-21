#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import os, os.path
import errno
import conf
import sys
import time

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
            sys.exit("%s is not permitted to use. " % (self.rootpath, ))

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
        """return infomation required in open().

        @param path
        @param flag
        @param mode
        @return (dest: file location ip,
        data_path: content path on data server, fsize: file size)
        """
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
        ret_dict = {}
        attrs = ("st_mode", "st_ino", "st_dev", "st_uid", "st_gid",
                 "st_nlink","st_size", "st_atime", "st_mtime", "st_ctime")
        st = os.lstat(self.rootpath + path)
        for attr in attrs:
            ret_dict[attr] = getattr(st, attr)
        if os.path.isfile(self.rootpath + path):
            ret_dict["st_size"] = self._get_metadata(path)[2]
        return ret_dict

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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        path TEXT,
        filename TEXT,
        par_id INT,
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

        self.db_cur.execute("""
        CREATE UNIQUE INDEX PATH_INDEX ON content (path)
        """)

        self.gid = os.getgid()
        self.uid = os.getuid()

        # make metadata of the directory
        current_t = int(time.time())
        self.db_cur.execute("""
        INSERT INTO content (
        path, mode, uid, gid, nlink, size, atime, mtime, ctime, dest, dest_path)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ('/', 16877, self.uid, self.gid, 1, 4096,
              current_t, current_t, current_t, 'None', 'None'))
        self.db_conn.commit()

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
        if mode == os.F_OK:
            return True

        assert len(l[0]) == 3
        (f_mode, f_uid, fmk_gid) = l[0]

        # check file mode and access mode
        if self.uid == f_uid:
            mode <<= 6
        elif self.gid == f_gid:
            mode <<= 3
        if f_mode & mode == mode:
            return True
        else:
            return False
        
    def getattr(self, path):
        st_dict = {}  # dict to return
        self.db_cur.execute("""
        SELECT mode, id, id, uid, gid, nlink, size, atime, mtime, ctime 
        FROM content WHERE path = ?""", (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)
        meta_t = l[0]
        attrs = ("st_mode", "st_ino", "st_dev", "st_uid", "st_gid",
                 "st_nlink","st_size", "st_atime", "st_mtime", "st_ctime")
        for i in range(10):
            st_dict[attrs[i]] = int(meta_t[i])
        return st_dict

    def readdir(self, path):
        ## TODO: clean up
        ret_list = []
        if len(path) != 1 and path[-1] == '/':
            path = path[:-1]
        self._ensure_permission_from_path(path, os.R_OK)
        self.db_cur.execute("""
        SELECT filename FROM content WHERE par_id
        in (SELECT id FROM content WHERE path = ?)""", (path, ))
        l = self.db_cur.fetchall()
        for rec in l:
            ret_list.append(rec[0])
        print ret_list
        return ret_list

    def mkdir(self, path, mode):
        create_mode = 16877   # default?
        size = 4096  # directory size
        current_t = int(time.time())
        nlink = 1
        pardir_path = os.path.dirname(path)

        self.db_cur.execute("""
        SELECT id, uid, gid, mode FROM content WHERE path = ?""",
                            (pardir_path, ))
        l = self.db_cur.fetchall()
        assert len(l) == 1, l
        assert len(l[0]) == 4
        (par_id, f_uid, f_gid, f_mode) = l[0]

        # check permission
        self._ensure_permission_from_mode(f_mode, f_uid, f_gid, os.W_OK)

        # check duplication
        r = self.db_cur.execute("SELECT * FROM content WHERE path = ?;",
                                (path, )).fetchone()
        if r:
            return self._raise_with_error(errno.EEXIST)

        # make metadata of the directory
        self.db_cur.execute("""
        INSERT INTO content (
        path, filename, par_id, mode, uid, gid, nlink, size, atime, mtime, ctime, dest, dest_path)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (path, os.path.basename(path), par_id, create_mode, self.uid, self.gid, nlink, size,
              current_t, current_t, current_t, 'None', 'None'))
        self.db_conn.commit()

    def rmdir(self, path):
        # check permission
        self._ensure_permission_from_path(os.path.dirname(path), os.W_OK)

        (dir_id, dir_mode) = self.db_cur.execute(
            "SELECT id, mode FROM content WHERE path = ?", (path, )).fetchone()

        # check if the directory has some contents
        r = self.db_cur.execute("SELECT * FROM content WHERE par_id = ?",
                                (dir_id, )).fetchone()
        if r:
            return self._raise_with_error(errno.ENOTEMPTY)

        # check if it is directory
        if self._is_directory(dir_mode) == False:
            self._raise_with_error(errno.ENOTDIR)

        self.db_cur.execute("""
        DELETE FROM content WHERE path = ?""", (path, ))
        self.db_conn.commit()
    
    def unlink(self, path):
        """Not implemented yet.
        """
        self.db_cur.execute("""
        SELECT mode, uid, gid, nlink, size, atime, mtime, ctime 
        FROM content WHERE path = ?""", (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)
            
    def rename(self, oldpath, newpath):

        ## extract a record and update (path)
        self.db_cur.execute("""
        SELECT mode, uid, gid, nlink, size, atime, mtime, ctime 
        FROM content WHERE path = ?""", (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)

    def chmod(self, path, mode):

        ## extract a record and update (mode)
        self.db_cur.execute("""
        UPDATE mode, uid, gid, nlink, size, atime, mtime, ctime 
        FROM content WHERE path = ?""", (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)

    def chown(self, path, uid, gid):
        ## extract a record and update (uid, gid)
        self.db_cur.execute("""
        SELECT mode, uid, gid, nlink, size, atime, mtime, ctime 
        FROM content WHERE path = ?""", (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)

    def truncate(self, path, length):
        ## extract a record and update (size)
        self._ensure_permission_from_path(path, os.W_OK)

    def utime(self, path, times):
        """
        @path file path to modify times
        @param times tuple of (atime, mtime)
        """
        pass

    def open(self, path, flag, mode):
        """return infomation required in open().

        @param path
        @param flag
        @param mode
        @return (dest: file location ip,
        data_path: content path on data server, fsize: file size)
        """
        pass

    def create(self, path, flag, mode, dest, data_path):
        """create file metadata and return information required in open().

        Nothing is returned by this function.

        @param path
        @param flag
        @param mode
        @param dest
        @param data_path
        """
        pass

    def _is_directory(self, mode):
        dir_flag = 1 << 13
        if dir_flag & mode == 0:
            return True
        else:
            return False

    def _ensure_permission_from_path(self, path, ch_mode):
        """
        @return 
        """
        self.db_cur.execute("""
        SELECT mode, uid, gid FROM content WHERE path = ?""", (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:            
            self._raise_with_error(errno.ENOENT)
        assert len(l[0]) == 3
        (f_mode, f_uid, f_gid) = l[0]
        # check file mode and access mode
        if self.uid == f_uid:
            ch_mode <<= 6
        elif self.gid == f_gid:
            ch_mode <<= 3
        if f_mode & ch_mode != ch_mode:
            self._raise_with_error(errno.EACCES)

    def _ensure_permission_from_mode(self, f_mode, f_uid, f_gid, ch_mode):
        """
        @return 
        """
        # check file mode and access mode
        if self.uid == f_uid:
            ch_mode <<= 6
        elif self.gid == f_gid:
            ch_mode <<= 3
        if f_mode & ch_mode != ch_mode:
            self._raise_with_error(errno.EACCES)

    def _raise_with_error(self, num):
        e = os.error(os.strerror(num))
        e.errno = num
        raise e

if __name__ == '__main__':
    import doctest
    doctest.testmod()
