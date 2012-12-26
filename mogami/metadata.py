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
        """wrapper of os.lstat but may change the file size.

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
        return os.mkdir(self.rootpath + path)

    def rmdir(self, path):
        return os.rmdir(self.rootpath + path)
    
    def unlink(self, path):
        dest = None
        dest_path = None

        if os.path.isfile(path):
            (dest, dest_path, fsize) = self._get_metadata(path)
        os.unlink(self.rootpath + path)

        return dest, dest_path
            
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
        """
        @param path path of file
        @param size truncate size
        @return (dest: file location, data_path: full path on data server)
        """
        with open(self.rootpath + path, 'r+') as f:
            (dest, data_path, org_size) = f.read().rsplit(',')
            org_size = int(org_size)
            f.truncate(0)
            f.seek(0)
            f.write("%s,%s,%d" % (dest, data_path, size))
        return dest, data_path

    def utime(self, path, times):
        return os.utime(self.rootpath + path, times)
    
    def addrep(self, path, dest, dest_path, f_size):
        f = open(self.rootpath + org, 'a')
        buf = "%s,%s,%d\n" % (dest, dest_path, f_size)
        f.write(buf)
        f.close()

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
    
        ## create a table for files
        self.db_cur.execute("""
        CREATE TABLE content (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        path TEXT,
        linkpath TEXT,
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

        ## create indexes (for path and par_id)
        self.db_cur.execute("""
        CREATE UNIQUE INDEX path_index ON content (path)
        """)
        self.db_cur.execute("""
        CREATE INDEX par_id_index ON content (par_id)
        """)

        ## remember my user id and group id
        self.uid = os.getuid()
        self.gid = os.getgid()

        ## make metadata of the root directory
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
    
    """the followings are handlers for each operation.

    * access
    * getattr (stat)
    * readdir
    * mkdir
    * rmdir
    * unlink
    * symlink
    * readlink
    * rename
    * chmod
    * chown
    * truncate
    * utime
    * open
    * release
    * create
    """
    def access(self, path, mode):
        """
        This function should return either True or False.

        @param path path of file to look for
        @param mode one of os.F_OK, os.R_OK, os.W_OK and os.X_OK
        @return True or False
        """
        self.db_cur.execute("""
        SELECT mode, uid, gid FROM content WHERE path = ?""", (path, ))
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
        ret_list = []

        if len(path) != 1 and path[-1] == '/':  # strip tail '/' from '****/'
            path = path[:-1]
        self.db_cur.execute("""
        SELECT mode, id, uid, gid FROM content WHERE path = ?""", (path, ))
        l = self.db_cur.fetchall()
        if len(l) == 0:
            self._raise_with_error(errno.ENOENT)
        (par_mode, par_id, par_uid, par_gid) = l[0]
        self._ensure_permission_from_mode(par_mode, par_uid, par_gid, os.R_OK)
        if self._is_directory(par_mode) == False:
            self._raise_with_error(errno.ENOTDIR)

        self.db_cur.execute("""
        SELECT filename FROM content WHERE par_id = ?""", (par_id, ))
        l = self.db_cur.fetchall()
        for rec in l:
            # need to be encoded to utf-8
            ret_list.append(rec[0].encode('utf-8'))
        return ret_list

    def mkdir(self, path):
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
        path, filename, par_id, mode, uid, gid, nlink, size,
        atime, mtime, ctime, dest, dest_path)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (path, os.path.basename(path), par_id, create_mode,
              self.uid, self.gid, nlink, size,
              current_t, current_t, current_t, 'None', 'None'))
        self.db_conn.commit()

    def rmdir(self, path):
        # check permission (parent dir)
        self._ensure_permission_from_path(os.path.dirname(path), os.W_OK)

        (dir_id, dir_mode) = self.db_cur.execute(
            "SELECT id, mode FROM content WHERE path = ?", (path, )).fetchone()

        # check if the directory has some contents
        r = self.db_cur.execute("SELECT * FROM content WHERE par_id = ?",
                                (dir_id, )).fetchone()
        if r:
            return self._raise_with_error(errno.ENOTEMPTY)

        # check if it is directory
        if self._is_directory(int(dir_mode)) == False:
            self._raise_with_error(errno.ENOTDIR)

        self.db_cur.execute("""
        DELETE FROM content WHERE path = ?""", (path, ))
        self.db_conn.commit()
    
    def unlink(self, path):
        # check permission (parent dir)
        self._ensure_permission_from_path(os.path.dirname(path), os.W_OK)

        dest = None
        dest_path = None

        self.db_cur.execute(
            "SELECT id, mode, dest, dest_path FROM content WHERE path = ?",
            (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)
        f_id = int(l[0][0])
        f_mode = int(l[0][1])

        if len(l[0][2]) > 0:
            dest = l[0][2]
            dest_path = l[0][3]

        # check if it is not directory
        if self._is_directory(int(f_mode)) == True:
            self._raise_with_error(errno.EISDIR)

        self.db_cur.execute("""
        DELETE FROM content WHERE id = ?""", (f_id, ))
        self.db_conn.commit()

        return dest, dest_path

    def symlink(self, frompath, topath):
        create_mode = 41471  # symlink
        size = len(frompath)
        current_t = int(time.time())
        nlink = 1
        pardir_path = os.path.dirname(topath)

        self.db_cur.execute("""
        SELECT id, uid, gid, mode FROM content WHERE path = ?""",
                            (pardir_path, ))
        l = self.db_cur.fetchall()
        assert len(l) == 1, l
        assert len(l[0]) == 4
        (par_id, par_uid, par_gid, par_mode) = l[0]

        # check permission
        self._ensure_permission_from_mode(par_mode, par_uid, par_gid, os.W_OK)

        # check duplication
        r = self.db_cur.execute("SELECT * FROM content WHERE path = ?;",
                                (topath, )).fetchone()
        if r:
            return self._raise_with_error(errno.EEXIST)

        # make metadata of the directory
        self.db_cur.execute("""
        INSERT INTO content (
        path, filename, linkpath, par_id, mode, uid, gid, nlink, size,
        atime, mtime, ctime)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (topath, os.path.basename(topath), frompath, par_id, create_mode,
              self.uid, self.gid, nlink, size, current_t, current_t, current_t))
        self.db_conn.commit()

    def readlink(self, path):
        self.db_cur.execute("""
        SELECT linkpath FROM content WHERE path = ?""",
                            (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)
        ret_path = l[0][0].encode('utf-8')
        if ret_path == '':
            self._raise_with_error(errno.EINVAL)
        return ret_path

    def rename(self, oldpath, newpath):
        ## ensure existence of the file or directory
        self.db_cur.execute("""
        SELECT id, mode, uid, gid FROM content WHERE path = ?""",
                            (oldpath, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)
        f_id = l[0][0]
        f_mode = int(l[0][1])
        f_uid = l[0][2]
        f_gid = l[0][3]
        self._ensure_permission_from_mode(f_mode, f_uid, f_gid, os.W_OK)
        self._ensure_permission_from_path(os.path.dirname(oldpath),
                                          os.W_OK and os.X_OK)
        (newpar_id, newpar_mode, newpar_uid, newpar_gid) = self.db_cur.execute(
            "SELECT id, mode, uid, gid FROM content WHERE path = ?",
            (os.path.dirname(newpath), )).fetchone()

        self._ensure_permission_from_mode(newpar_mode, newpar_uid,
                                          newpar_gid, os.W_OK and os.X_OK)

        ## check existing content
        self.db_cur.execute("""
        SELECT id, mode FROM content WHERE path = ?""",
                            (newpath, ))
        l = self.db_cur.fetchall()
        if len(l) != 0:
            for org_content in l:
                if self._is_directory(int(l[0][1])) != self._is_directory(f_mode):
                    if self._is_directory(f_mode) == True:
                        self._raise_with_error(errno.ENOTDIR)
                    else:
                        self._raise_with_error(errno.EISDIR)
                else:
                    self.db_cur.execute("""
                    DELETE FROM content WHERE id = ?""", (l[0][0], ))
        ## finally rename the content
        self.db_cur.execute("""
        UPDATE content SET path = ?, filename = ?, par_id = ? WHERE id = ?""",
                            (newpath, os.path.basename(newpath), newpar_id, f_id))        

        ## apply the change to children
        children_list = []
        self.db_cur.execute("SELECT id, mode, path FROM content WHERE par_id = ?",
                            (f_id, ))
        l = self.db_cur.fetchall()
        for child in l:
            children_list.append((int(child[0]), int(child[1]), child[2]))

        depth_max = 1000  # tentative
        depth_num = 0

        while len(children_list) > 0 or depth_num > depth_max:
            child = children_list.pop(0)
            self.db_cur.execute("""
            UPDATE content SET path = ? WHERE id = ?""",
                                (child[2].replace(oldpath, newpath, 1), child[0]))
            if self._is_directory(child[1]) == True:
                self.db_cur.execute("SELECT id, mode, path FROM content WHERE par_id = ?",
                                    (child[0], ))
                l = self.db_cur.fetchall()
                for grand_child in l:
                    children_list.append((int(grand_child[0]),
                                          int(grand_child[1]), grand_child[2]))
                depth_num += 1
        self.db_conn.commit()

    def chmod(self, path, mode):
        ## ensure existence of the file or directory
        self.db_cur.execute("""
        SELECT id, uid FROM content WHERE path = ?""",
                            (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)
        f_id = l[0][0]
        f_uid = int(l[0][1])
        if self.uid != 0 and f_uid != self.uid:
            self._raise_with_error(errno.EPERM)
        ## update mode value
        self.db_cur.execute("""
        UPDATE content SET mode = ? WHERE id = ?""", (mode, f_id))
        self.db_conn.commit()

    def chown(self, path, uid, gid):
        ## ensure existence of the file
        self.db_cur.execute("""
        SELECT id FROM content WHERE path = ?""",
                            (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)
        f_id = l[0][0]
        ## ensure requestor is superuser
        ## (chwon is allowed only for superuser)
        if self.uid != 0:
            self._raise_with_error(errno.EPERM)
        ## extract a record and update (uid, gid)
        self.db_cur.execute("""
        UPDATE content SET uid = ?, gid = ? WHERE id = ?""",
                            (uid, gid, f_id))
        self.db_conn.commit()

    def truncate(self, path, length):
        """
        @param path path of file
        @param size truncate size
        @return (dest: file location, data_path: full path on data server)
        """
        ## ensure existence of the file
        self.db_cur.execute("""
        SELECT id, mode, uid, gid, dest, dest_path FROM content WHERE path = ?""",
                            (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)
        assert len(l) == 1

        f_id = int(l[0][0])
        f_mode = int(l[0][1])
        f_uid = int(l[0][2])
        f_gid = int(l[0][3])
        f_dest = l[0][4]
        f_dest_path = l[0][5]

        # check if it is not directory
        if self._is_directory(int(f_mode)) == True:
            self._raise_with_error(errno.EISDIR)

        # check permission
        self._ensure_permission_from_mode(f_mode, f_uid, f_gid, os.W_OK)

        ## finally update the values
        self.db_cur.execute("""
        UPDATE content SET size = ? WHERE id = ?""",
                            (length, f_id))
        self.db_conn.commit()

    def utime(self, path, times):
        """
        @path file path to modify times
        @param times tuple of (atime, mtime)
        """
        if times:
            (atime, mtime) = times
        else:
            current_t = int(time.time())
            (atime, mtime) = (current_t, current_t)

        ## ensure existence of the file
        self.db_cur.execute("""
        SELECT id, mode, uid, gid FROM content WHERE path = ?""",
                            (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)
        f_id = l[0][0]
        ## TODO: add permission check here?

        ## finally update the values
        self.db_cur.execute("""
        UPDATE content SET atime = ?, mtime = ? WHERE id = ?""",
                            (atime, mtime, f_id))
        self.db_conn.commit()


    def open(self, path, flag, mode):
        """return infomation required in open().

        @param path
        @param flag
        @param mode
        @return (dest: file location ip,
        data_path: content path on data server, fsize: file size)
        """
        self.db_cur.execute("""
        SELECT id, uid, gid, size, mode, dest, dest_path
        FROM content WHERE path = ?""", (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            self._raise_with_error(errno.ENOENT)
        (f_id, f_uid, f_gid, f_size, f_mode, f_dest, f_dest_path) = l[0]
        
        ## TODO: check permission
        return f_dest, f_dest_path, int(f_size)

    def release(self, path, fsize):
        """release handler for metadata

        actually, in this function only file size is changed.
        @param path
        @param fsize
        """
        ## extract a record and update (uid, gid)
        self.db_cur.execute("""
        UPDATE content SET size = ? WHERE path = ?""",
                            (fsize, path))
        self.db_conn.commit()

    def create(self, path, flag, mode, dest, dest_path):
        """create file metadata and return information required in open().

        Nothing is returned by this function.

        @param path
        @param flag
        @param mode
        @param dest
        @param data_path
        """
        if len(mode) == 0:
            create_mode = 33188  # default
        else:
            create_mode = mode[0]
        size = 0  # size is originally 0
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
        path, filename, par_id, mode, uid, gid, nlink, size,
        atime, mtime, ctime, dest, dest_path)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (path, os.path.basename(path), par_id, create_mode,
              self.uid, self.gid, nlink, size,
              current_t, current_t, current_t, dest, dest_path))
        self.db_conn.commit()

    def _is_directory(self, mode):
        dir_flag = 1 << 15
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
