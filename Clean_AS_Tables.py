#!/usr/bin/python3

import aerospike
import sys

print("Current Python version: ", sys.version_info[0],".",sys.version_info[1])


# ***Open communication with the database***

# Specify the IP addresses and ports for the Aerospike cluster
config = {
  'hosts': [ ('18.222.211.167', 3000), ('3.16.180.184', 3000) ]
}
# Create a client and connect it to the cluster
try:
  client = aerospike.client(config).connect()
except:
  print("failed to connect to the cluster with", config['hosts'])
  sys.exit(1)

def show_key(r_tuple):
    try:
        client.remove(r_tuple[0]) # the value passed by foreach is an n-tuple where the key is element 0
    except:
        print("Couldn't remove ", r_tuple)
    return

for table in ["Table1A", "Table1B", "Table2A", "Table2B", "Table3A", "Table3B", "Table3C", "FK_BtoA_FKTable", "FK_CtoA_FKTable", "FK_CtoB_FKTable"]:
    currentDBscan = client.scan("test_DB_SEF", table)
    print("Removing Table:", table)
    #res = currentDBscan.results()
    scan_opts = {
    #'concurrent': True,
    'nobins': True,
    'priority': aerospike.SCAN_PRIORITY_MEDIUM
    }
    currentDBscan.foreach(show_key, options=scan_opts)
