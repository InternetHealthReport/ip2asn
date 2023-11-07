import bz2
from confluent_kafka import Consumer, KafkaError
import json
import msgpack
import os
import pickle
import radix
import sys


class ip2asn:
    def __init__(self, db="db/latest.pickle", ixp=None, kafka_topic='', kafka_host='localhost:9092'):

        self.db = db
        self.asname = {}
        self.rtree = radix.Radix()

        if kafka_topic:
            # Get data from Kafka topic
            c = Consumer({
                'bootstrap.servers': kafka_host,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
                'group.id': 'ip2asn_consumer',
            })
            c.subscribe([kafka_topic])

            while True:
                msg = c.poll(timeout=10.0)

                if msg is None:
                    break

                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    continue

                msg = {
                       'prefix': msg.key().decode('utf-8'),
                       'data': msgpack.unpackb(msg.value(), raw=False)
                       }

                rnode = self.rtree.add(msg['prefix'])
                for k, v in msg['data'].items():
                    rnode.data[k] = v

            c.close()
        else:
            # get data from local files
            self.rtree = pickle.load(bz2.BZ2File(db, "rb"))

            if ixp is not None:
                with open(ixp) as fi:
                    for line in fi:
                        if not line.startswith("#"):
                            ix = json.loads(line)
                            for pfx in ix["prefixes"]["ipv4"]:
                                node = self.rtree.add(pfx)
                                # negative values for IXPs
                                asn = ix["ix_id"]*-1
                                node.data["as"] = asn
                                self.asname[asn] = ix["name"]

                            for pfx in ix["prefixes"]["ipv6"]:
                                node = self.rtree.add(pfx)
                                # negative values for IXPs
                                asn = ix["ix_id"]*-1
                                node.data["as"] = asn
                                self.asname[asn] = ix["name"]


    def ip2asn(self, ip):
        """ Find the ASN corresponding to the given IP address"""
        try:
            node = self.rtree.search_best(ip)
        except ValueError:
            print("Wrong IP address: %s" % ip)
            return 0

        if node is None:
            return 0
        else:
            asn = node.data["as"]
            # if we have a set of ASN we use only the first ASN
            if isinstance(asn, str) and asn.startswith("{"):
                asn = asn.replace("{","[").replace("}", "]")
                asn = json.loads(asn)[0]

            return int(asn)

    def covering_prefixes(self, ip):
        """ Find all prefixes covering the given IP address"""
        try:
            node = self.rtree.search_best(ip)
        except ValueError:
            print("Wrong IP address: %s" % ip)
            return None

        parents = [node.prefix]
        parent = node.parent
        while parent:
            parents.append(parent.prefix)
            parent = parent.parent

        return parents

    def ip2prefix(self, ip):
        try:
            node = self.rtree.search_best(ip)
        except ValueError:
            print("Wrong IP address: %s" % ip)
            return None

        if node is None:
            return None
        else:
            return node.prefix


    def asn2name(self, asn):
        if asn in self.asname:
            return self.asname[asn]
        else:
            return ""

if __name__ == "__main__":

    if len(sys.argv)<3:
        print("usage: %s yyyymm ip")
        sys.exit()
    
    ia = ip2asn("db/rib.%s01.pickle.bz2" % sys.argv[1])
    asn = ia.ip2asn(sys.argv[2])
    prefix = ia.ip2prefix(sys.argv[2])
    if asn is None:
        print("Unknown")
    else:
        print(prefix)
        print(asn)
