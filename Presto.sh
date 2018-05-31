#!/bin/bash
#    Copyright 2015 Google, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
set -x -e

# Configure local ssds
# Combine all SSDs into a single, RAID-0 volume
mdadm --create /dev/md0 \
    --level=0 \
    --raid-devices=4 \
    /dev/nvme0n1 \
    /dev/nvme0n2 \
    /dev/nvme0n3 \
    /dev/nvme0n4 
# Format the volume
mkfs.ext4 -F /dev/md0
# Mount it
mkdir -p /mnt/disks/ssd-array
mount /dev/md0 /mnt/disks/ssd-array
chmod a+w /mnt/disks/ssd-array

# Variables for running this script
ROLE=$(curl -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/attributes/PrestoRole)
HOSTNAME=$(hostname)
DNSNAME=$(dnsdomainname)
FQDN=${HOSTNAME}.${DNSNAME}
PRESTO_MASTER=$(curl -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/attributes/PrestoMaster)
PRESTO_MASTER_FQDN=${PRESTO_MASTER}.${DNSNAME}
HIVE_FQDN=tpcds-hive-m.${DNSNAME}
PRESTO_VERSION="0.202"
HTTP_PORT="8080"
TASKS_PER_INSTANCE_PER_QUERY=64
INSTANCE_MEMORY=120000
PRESTO_JVM_MB=$(( ${INSTANCE_MEMORY} * 8 / 10 ))
PRESTO_OVERHEAD=500
PRESTO_QUERY_NODE_MB=$(( (${PRESTO_JVM_MB} - ${PRESTO_OVERHEAD}) * 7 / 10 ))
PRESTO_RESERVED_SYSTEM_MB=$(( (${PRESTO_JVM_MB} - ${PRESTO_OVERHEAD}) * 3 / 10 ))

# Prevents "Too many open files"
ulimit -n 30000

# Install Java
apt-get install openjdk-8-jre-headless -y

# Download and unpack Presto server
wget https://s3.us-east-2.amazonaws.com/starburstdata/presto/starburst/195e/0.195-e.0.5/presto-server-0.195-e.0.5.tar.gz
tar -zxvf presto-server-0.195-e.0.5.tar.gz

# Install cli
if [[ "${ROLE}" == 'Master' ]]; then
	wget https://s3.us-east-2.amazonaws.com/starburstdata/presto/starburst/195e/0.195-e.0.5/presto-cli-0.195-e.0.5-executable.jar -O /usr/bin/presto
	chmod a+x /usr/bin/presto
	apt-get install unzip
fi

# Copy GCS connector
wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar -O presto-server-0.195-e.0.5/plugin/hive-hadoop2/gcs-connector-latest-hadoop2.jar 

# Configure Presto
mkdir presto-server-0.195-e.0.5/etc
mkdir presto-server-0.195-e.0.5/etc/catalog

cat > presto-server-0.195-e.0.5/etc/node.properties <<EOF
node.environment=production
node.id=$(uuidgen)
node.data-dir=/var/presto/data
EOF

# Configure hive metastore
cat > presto-server-0.195-e.0.5/etc/catalog/hive.properties <<EOF
connector.name=hive-hadoop2
hive.metastore.uri=thrift://${HIVE_FQDN}:9083
hive.parquet-optimized-reader.enabled=true
hive.parquet-predicate-pushdown.enabled=true
hive.non-managed-table-writes-enabled=true
EOF

# Configure GCS connector
# TODO fiddle with caching options in https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/gcs/src/main/java/com/google/cloud/hadoop/fs/gcs/GoogleHadoopFileSystemBase.java
mkdir -p /etc/hadoop/conf
cat > /etc/hadoop/conf/core-site.xml <<EOF
<?xml version="1.0" ?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.gs.project.id</name>
    <value>digital-arbor-400</value>
  </property>
</configuration>
EOF

cat > presto-server-0.195-e.0.5/etc/jvm.config <<EOF
-server
-Xmx${PRESTO_JVM_MB}m
-Xmn512m
-XX:+UseG1GC
-XX:+ExplicitGCInvokesConcurrent
-XX:+AggressiveOpts
-XX:+HeapDumpOnOutOfMemoryError
-XX:OnOutOfMemoryError=kill -9 %p
-Dhive.config.resources=/etc/hadoop/conf/core-site.xml
-Djava.library.path=/usr/lib/hadoop/lib/native/:/usr/lib/
-Dcom.sun.management.jmxremote 
-Dcom.sun.management.jmxremote.ssl=false 
-Dcom.sun.management.jmxremote.authenticate=false 
-Dcom.sun.management.jmxremote.port=10999 
-Dcom.sun.management.jmxremote.rmi.port=10999 
-Djava.rmi.server.hostname=127.0.0.1 
EOF

mkdir -p /mnt/disks/ssd-array/raptor
if [[ "${ROLE}" == 'Master' ]]; then
	# Configure master properties
	cat > presto-server-0.195-e.0.5/etc/config.properties <<EOF
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=${HTTP_PORT}
query.max-memory=999TB
query.max-memory-per-node=${PRESTO_QUERY_NODE_MB}MB
resources.reserved-system-memory=${PRESTO_RESERVED_SYSTEM_MB}MB
discovery-server.enabled=true
discovery.uri=http://${PRESTO_MASTER_FQDN}:${HTTP_PORT}
query.max-history=1000
task.concurrency=${TASKS_PER_INSTANCE_PER_QUERY}
EOF
else
	cat > presto-server-0.195-e.0.5/etc/config.properties <<EOF
coordinator=false
http-server.http.port=${HTTP_PORT}
query.max-memory=999TB
query.max-memory-per-node=${PRESTO_QUERY_NODE_MB}MB
resources.reserved-system-memory=${PRESTO_RESERVED_SYSTEM_MB}MB
discovery.uri=http://${PRESTO_MASTER_FQDN}:${HTTP_PORT}
query.max-history=1000
task.concurrency=${TASKS_PER_INSTANCE_PER_QUERY}
EOF
fi

cat > presto-server-0.195-e.0.5/etc/catalog/memory.properties <<EOF
connector.name=memory
memory.max-data-per-node=10GB
EOF

# Start presto
presto-server-0.195-e.0.5/bin/launcher start