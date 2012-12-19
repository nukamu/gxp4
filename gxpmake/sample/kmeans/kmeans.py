#!/usr/bin/python
import os,random,string,sys,time
import mapred

def Ws(s):
    sys.stdout.write(s)

def Es(s):
    sys.stderr.write(s)

class point:
    def __init__(self, x, y, z, weight, changed):
        self.x = x
        self.y = y
        self.z = z
        self.weight = weight
        self.changed = changed

    def __add__(self, p):
        self.x += p.x
        self.y += p.y
        self.z += p.z
        self.weight += p.weight
        self.changed += p.changed
        return self


class kmeans_job:
    def __init__(self, tmp_dir, centers):
        self.tmp_dir = tmp_dir
        self.centers = centers
        self.p_template = ("%s/points.%%d" % self.tmp_dir)
        self.x_template = ("%s/pointsx.%%d" % self.tmp_dir)

    def parse_line(self, line):
        try:
            [ i,c,x,y,z ] = line.split()
        except ValueError,e:
            print line
            raise e
        i = int(i)
        c = int(c)
        x = float(x)
        y = float(y)
        z = float(z)
        return i,c,x,y,z

    def nearest(self, x, y, z):
        min_d2 = float("inf")
        min_c = None
        for c,(cx,cy,cz) in self.centers.items():
            d2 = (x - cx)**2 + (y - cy)**2 + (z - cz)**2
            if d2 < min_d2:
                min_d2 = d2
                min_c = c
        return min_c

    def map_begin(self, R):
        self.p_file = self.p_template % R.mapper_idx
        self.x_file = self.x_template % R.mapper_idx
        self.wp = open(self.x_file, "wb")

    def map_end(self, R):
        self.wp.close()
        os.rename(self.x_file, self.p_file)

    def mapf(self, line, R, *opts):
        i,c,x,y,z = self.parse_line(line)
        c_ = self.nearest(x, y, z)
        changed = 0
        if c != c_: changed = 1
        R.add(c_, point(x, y, z, 1, changed))
        self.wp.write("%d %d %f %f %f\n" % (i, c_, x, y, z))

    def reducef(self, c, p):
        n = p.weight
        print c,p.x/n,p.y/n,p.z/n,p.changed

    def job(self):
        mapred.become_job(map_fun=self.mapf, 
                          map_begin=self.map_begin,
                          map_end=self.map_end,
                          reduce_fun=self.reducef)

def parse_centers(centers_s):
    """
    cluster_id,x,y,z:cluster_id,x,y,z:cluster_id,x,y,z:...
    """
    ps = centers_s.split(":")
    centers = {}
    for p in ps:
        [ cluster,x,y,z ] = p.split(",")
        centers[int(cluster)] = (float(x), float(y), float(z))
    return centers

def unparse_centers(centers):
    """
    cluster_id,x,y,z:cluster_id,x,y,z:cluster_id,x,y,z:...
    """
    s = []
    items = centers.items()
    items.sort()
    for i,(x,y,z) in items:
        s.append("%d,%f,%f,%f" % (i, x, y, z))
    return string.join(s, ":")

def parse_reduce_out(s):
    centers = {}
    changed = 0
    for line in s.split("\n"):
        if line == "": continue
        [ cluster_id, x, y, z, ch ] = line.split()
        centers[int(cluster_id)] = (float(x), float(y), float(z))
        if int(ch): changed = 1
    return centers,changed

def iterate(centers, files, tmp_dir, partitioning, n_mappers, n_reducers, affinity):
    t0 = time.time()
    centers_s = unparse_centers(centers)
    status,out,err,where = mapred.sched(cmd=("./kmeans.py %s %s" % (tmp_dir, centers_s)),
                                        input_files=files, 
                                        n_mappers=n_mappers, 
                                        n_reducers=n_reducers,
                                        load_balancing_scheme=partitioning,
                                        affinity=affinity)
    if status != 0: return None,None,None,None
    files = string.join([ ("%s/points.%d" % (tmp_dir, i)) for i in range(n_mappers) ], ":")
    centers,changed = parse_reduce_out(out)
    t1 = time.time()
    Ws("iterate: %f sec\n" % (t1 - t0))
    return centers,files,changed,where

def mk_affinity(where, n_mappers, n_reducers):
    affinity = {}
    for i in range(n_mappers):
        affinity["map",i] = where[i][0]
    for i in range(n_reducers):
        affinity["red",i] = where[n_mappers+i][0]
    return affinity

def run(K, input_files, tmp_dir, n_mappers, n_reducers):
    t0 = time.time()
    R = random.Random()
    R.seed(0)
    centers = {}
    for i in range(K):
        centers[i] = (R.random(), R.random(), R.random())
    centers,files,changed,where = iterate(centers, input_files, tmp_dir, "block", n_mappers, n_reducers, {})
    if centers is None: 
        Es("initial iteration failed\n")
        return 1
    affinity = mk_affinity(where, n_mappers, n_reducers)
    for i in range(100):
        Ws("iteration %d\n" % i)
        if centers is None: 
            Es("iteration %d failed\n" % i)
            return 1
        if changed == 0: break
        # print centers,files,changed
        centers,files,changed,where = iterate(centers, files, tmp_dir, "file", n_mappers, n_reducers, affinity)
    t1 = time.time()
    Ws("total: %f sec\n" % (t1 - t0))
    return 0

def main():
    if os.environ.has_key("MAPRED_JOB"):
        tmp_dir = sys.argv[1]
        centers = parse_centers(sys.argv[2])
        kj = kmeans_job(tmp_dir, centers)
        kj.job()
        return 0
    else:
        n_mappers = 1
        n_reducers = 1
        K = 3
        input_files = [ "points" ]
        tmp_dir = "/tmp/tau"
        if len(sys.argv) > 1: n_mappers = int(sys.argv[1])
        if len(sys.argv) > 2: K = int(sys.argv[2])
        if len(sys.argv) > 3: tmp_dir = sys.argv[3]
        if len(sys.argv) > 4: input_files = sys.argv[4:]
        return run(K, input_files, tmp_dir, n_mappers, n_reducers)

    
if __name__ == "__main__":
    sys.exit(main())

#
# kmeans.py :
#   x,y,z 
#

def serial_kmeans(points_file, initial_centers, K):
    centers = initial_centers
    while 1:
        changed = 0
        new_centers = {}
        for cluster in range(K):
            new_centers[cluster] = ((0.0, 0.0, 0.0), 0)
        new_points = []
        for i,cluster,p in points:
            new_cluster = nearest_cluster(p, centers)
            new_points.append((i, new_cluster, p))
            (X,Y,Z),n = new_centers[new_cluster]
            new_centers[new_cluster] = ((x+X,y+Y,z+Z), n+1)
            if cluster != new_cluster: changed = 1
        if changed == 0: break
        for cluster,((X,Y,Z),n) in new_centers.items():
            centers[cluster] = (X / n, Y / n, Z / n)
