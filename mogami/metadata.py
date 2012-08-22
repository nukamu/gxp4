#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import os, os.path
import errno


class MogamiMetaFS(object):
    """Class for managing metadata on local file system (like ext4).

    supposed to be used in metadata server's daemon.
    """
    def __init__(self, dir_path):
        # rootpath must be absolute path
        # tail string must not '/'
        self.rootpath = dir_path

    def _get_metadata(self, path):
        """read metadata from file

        this function must return tuple with three elements (None with error)
        @param path path of file to get metadata
        """
        with open(path, 'r') as f:
            # read all data from file
            read_buf = f.read()
        try:
            # metadata is separated by ','
            (dest, datapath, fsize) = buf.rsplit(',')
            return (dest, datapath, fsize)
        except ValueError, e:
            return (None, None, None)

    def access(self, path, mode):
        
        pass

    def getattr(self, path):
        """

        @path path of file to get attributes
        @return tuple of st(result of stat) and fsize
        """
        st = os.lstat(self.rootpath + path)
        if os.path.isfile(path):
            fsize = _get_metadata(path)[2]
        else:
            fsize = -1
        return (st, fsize)

    def readdir(self, path):
        return os.listdir(self.rootpath + path)

    def mkdir(self, path):
        return os.mkdir(self.rootpath + path)

    def rmdir(self, path):
        return os.rmdir(self.rootpath + path)
    
    def unlink(self, path):
        pass
            
    def rename(self, oldpath, newpath):
        pass

    def chmopd(self, path, mode):
        pass

    def chown(self, path, uid, gid):
        pass

    def truncate(self, path, size):
        pass

    def utime(self, path, times):
        pass


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
    
        # Create files table
        self.db_cur.execute("""
        CREATE TABLE files (
        path TEXT PRIMARY KEY,
        mode INT,
        uid INT,
        gid INT,
        nlink INT,
        size INT,
        atime INT,
        mtime INT,
        ctime INT,
        dist TEXT,
        dist_path TEXT
        )
        """)

    def _set_file(self, path, st, dist, dist_path):
        """register metadata of files to DB

        @param path path name
        @param st same with the return value of os.lstat(path)
        @param dist ファイルを持っているメタデータサーバのIP
        @param dist_path メタデータサーバ上でのpath
        @return On success, zero is returned. On error, error number is returned.
        """
        r = self.db_cur.execute("SELECT * FROM files WHERE path = ?;", (path,)).fetchone()
        if r:
            return errno.EEXIST
        self.db_cur.execute("""
        INSERT INTO files (
        path, mode, uid, gid, nlink, size, atime, mtime, ctime, dist, dist_path) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (path, st.st_mode, st.st_uid, st.st_gid, st.st_nlink, st.st_size,
              st.st_atime, st.st_mtime, st.st_ctime, dist, dist_path ))
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
            path, mode, uid, """)

    def _rm_dir(self, path):
        r = self.db_cur.execute("", (path))

    def dump_all(self, ):
        """print information of all files
        """
        self.db_cur.execute("""
        SELECT * FROM files""")
        l = self.db_cur.fetchall()
        for tmp in l:
            for tmp2 in tmp:
                print tmp2,
            print ""
    
    def return_st(self, path):
        """return metadata statement of a file

        this function can be used as os.lstat
        return None, if file doesn't exist

        @param path file path
        """
        self.db_cur.execute("""
        SELECT mode, uid, gid, nlink, size, atime, mtime, ctime 
        FROM files WHERE path = ?""", (path, ))
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

    def return_dist(self, path):
        """will return which data server has required file.

        @param path path of required file
        @return [ip_of_data_server, file_path_on_the_data_server]
        """
        self.db_cur.execute("""
        SELECT dist, dist_path 
        FROM files WHERE path = ?""", (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            return None
        ret = [str(l[0][0]), str(l[0][1])]
        return ret

    def access(self, path, mode):
        pass

    def getattr(self, path):
        pass

    def readdir(self, path):
        pass

    def mkdir(self, path):
        pass

    def rmdir(self, path):
        pass
    
    def unlink(self, path):
        pass
            
    def rename(self, oldpath, newpath):
        pass

    def chmod(self, path, mode):
        pass

    def chown(self, path, uid, gid):
        pass

    def truncate(self, path, uid, gid):
        pass

    def utime(self, path, times):
        pass


if __name__ == '__main__':
    import doctest
    doctest.testmod()
