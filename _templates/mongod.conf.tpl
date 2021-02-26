# mongod.conf

# for documentation of all options, see:
#   http://docs.mongodb.org/manual/reference/configuration-options/

# Where and how to store data.
storage:
  dbPath: {{ .Path }}
  journal:
    enabled: true
#  engine:
#  mmapv1:
#  wiredTiger:

# network interfaces
net:
  port: {{ .Port }}
  bindIp: {{ .IP }}

# how the process runs
processManagement:
  timeZoneInfo: /usr/share/zoneinfo

#security:

#operationProfiling:

#replication:

#sharding:
#  clusterRole: shardsvr

## Enterprise-Only Options:

#auditLog:

#snmp:
