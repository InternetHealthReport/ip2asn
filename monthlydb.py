import sys
import datetime
import urllib.request
import readrib
import os
import git
import errno

def symlink_force(target, link_name):
    try:
        os.symlink(target, link_name)
    except OSError as e:
        if e.errno == errno.EEXIST:
            os.remove(link_name)
            os.symlink(target, link_name)
        else:
            raise e

url ="http://archive.routeviews.org/route-views.wide/bgpdata/%s.%02d/RIBS/rib.%s%02d01.0000.bz2"

today = None
if len(sys.argv) < 3:
    today = datetime.datetime.utcnow()
    month = today.month
    year = today.year
else:
    year = int(sys.argv[1])
    month = int(sys.argv[2])

completeurl = url % (year, month, year, month)
fname = completeurl.rpartition("/")[2]
try:
    os.mkdir("tmp")
except OSError:
    pass

print("Downloading %s..." % completeurl)
urllib.request.urlretrieve(completeurl, "tmp/"+fname )

rr = readrib.readrib("tmp/"+fname)
rr.loadData()
rr.savertree()

symlink_force(rr.picklefile, "db/latest.pickle")
g = git.cmd.Git("./")
g.pull()
g.add(rr.picklefile)
g.commit('-am "add new pickle file (%s)"' % rr.picklefile)
g.push()
os.remove("tmp/"+fname)
