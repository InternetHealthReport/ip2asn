# ip2asn
Simple tool that matches IP addresses to ASN at any point in time.

**Data structures are now compressed, if you used an older version of this repo you should clone it again.**

# Example
Find the ASN for 8.8.8.8 (on Jan. 2017):
```
» python3 ip2asn.py 201701 8.8.8.8
15169
```
Yes, that's Google, but that wasn't like that in 2009:
```
» python3 ip2asn.py 200901 8.8.8.8
3356
```

### Need more older/newer files?
Create data structures from RIB files (assume route-views file name but this can be easily changed) with this command:
``python3 readrib.py rib.20170401.0000.bz2``

### Need to map a lot of IPs?
Look at ip2asn.py. You don't need to load the datastructure each time you query an IP. Copy/paste the ip2asn.py code and call the ip2asn function as much as you like.
