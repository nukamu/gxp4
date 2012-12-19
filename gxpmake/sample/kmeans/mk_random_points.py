#!/usr/bin/python
import math,random,sys

def mk_point(R):
    x = R.random()
    y = R.random()
    z = R.random()
    return x,y,z

def mk_gauss_point(R, center, sigma):
    r = R.gauss(0.0, sigma)
    theta = R.uniform(0, math.pi)
    cx,cy,cz = center
    return (cx + r * math.cos(theta),
            cy + r * math.sin(theta),
            0.0)

def main():
    n_points = 100
    n_clusters = 3
    R = random.Random()
    R.seed(0)
    if len(sys.argv) > 1: n_points = int(sys.argv[1])
    centers = {}
    for c in range(n_clusters):
        centers[c] = mk_point(R)
    for i in range(n_points):
        c = R.randint(0, n_clusters-1)
        x,y,z = mk_gauss_point(R, centers[c], 0.1)
        print i,-1,x,y,z

if __name__ == "__main__":
    main()
