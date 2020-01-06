import sys
import os
from subprocess import Popen, PIPE
import radix
import pickle
import bz2


class readrib:
    def __init__(self, fname):
        self.rtree = radix.Radix()
        self.fname = fname
        self.picklefile = None

    def loadData(self, path=False):
        print("reading : "+self.fname)
        p1 = Popen(["bgpdump", "-m", "-v", "-t", "change", self.fname], stdout=PIPE, 
                bufsize=0, encoding='utf-8')

        for line in p1.stdout: 
            res = line.split('|',15)
            zTd, zDt, zS, zOrig, zAS, zPfx, sPath, zPro, zOr, z0, z1, z2, z3, z4, z5 = res
            
            if zPfx == "0.0.0.0/0" or zPfx == "::/0":
                continue

            node = self.rtree.add(zPfx)
            node.data["as"] = sPath.split(" ")[-1]
            if path:
                node.data["path"] = sPath


    def savertree(self): 
        self.picklefile = "db/"+self.fname.rpartition("/")[2][:-9]+".pickle.bz2"
        sys.stderr.write("Computing %s... \n" % self.picklefile)
        fp = bz2.BZ2File(self.picklefile, 'wb')
        pickle.dump(self.rtree, fp, 2)


if __name__ == "__main__":
    if len(sys.argv) < 2:
            print("usage: %s ribfile" % sys.argv[0])
            sys.exit()

    rr = readrib(sys.argv[1])
    rr.loadData()
    rr.savertree()
