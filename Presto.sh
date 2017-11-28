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

# Variables for running this script
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
HOSTNAME=$(hostname)
DNSNAME=$(dnsdomainname)
FQDN=${HOSTNAME}.$DNSNAME
CONNECTOR_JAR=$(find /usr/lib/hadoop/lib -name 'gcs-connector-*.jar')
PRESTO_VERSION="0.185"
HTTP_PORT="8080"
CORES_PER_INSTANCE=4

# Download and unpack Presto server
wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz
#gsutil cp gs://fivetran-benchmark/presto-server-${PRESTO_VERSION}.tar.gz .
tar -zxvf presto-server-${PRESTO_VERSION}.tar.gz
mkdir /var/presto
mkdir /var/presto/data

# Copy required Jars
cp ${CONNECTOR_JAR} presto-server-${PRESTO_VERSION}/plugin/hive-hadoop2

# Configure Presto
mkdir presto-server-${PRESTO_VERSION}/etc
mkdir presto-server-${PRESTO_VERSION}/etc/catalog

cat > presto-server-${PRESTO_VERSION}/etc/node.properties <<EOF
node.environment=production
node.id=$(uuidgen)
node.data-dir=/var/presto/data
EOF

# TODO - Inspect /etc/hive/conf/hite-site.xml to pull this uri
cat > presto-server-${PRESTO_VERSION}/etc/catalog/hive.properties <<EOF
connector.name=hive-hadoop2
hive.metastore.uri=thrift://localhost:9083
hive.parquet-optimized-reader.enabled=true
hive.parquet-predicate-pushdown.enabled=true
hive.non-managed-table-writes-enabled=true
EOF

# Allocate 80% of system memory to Presto
PRESTO_JVM_MB=$(( 15075 * 8 / 10 ))

# Allocate JVM memory to overhead / (70% query / 30% system)
PRESTO_OVERHEAD=500
PRESTO_QUERY_NODE_MB=$(( (${PRESTO_JVM_MB} - ${PRESTO_OVERHEAD}) * 7 / 10 ))
PRESTO_RESERVED_SYSTEM_MB=$(( (${PRESTO_JVM_MB} - ${PRESTO_OVERHEAD}) * 3 / 10 ))

cat > presto-server-${PRESTO_VERSION}/etc/jvm.config <<EOF
-server
-Xmx${PRESTO_JVM_MB}m
-Xmn512m
-XX:+UseG1GC
-XX:+ExplicitGCInvokesConcurrent
-XX:+AggressiveOpts
-XX:+HeapDumpOnOutOfMemoryError
-XX:OnOutOfMemoryError=kill -9 %p
-Dhive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml
-Djava.library.path=/usr/lib/hadoop/lib/native/:/usr/lib/
-Dcom.sun.management.jmxremote 
-Dcom.sun.management.jmxremote.ssl=false 
-Dcom.sun.management.jmxremote.authenticate=false 
-Dcom.sun.management.jmxremote.port=10999 
-Dcom.sun.management.jmxremote.rmi.port=10999 
-Djava.rmi.server.hostname=127.0.0.1 
EOF

if [[ "${ROLE}" == 'Master' ]]; then
	# Configure master properties
	PRESTO_MASTER_FQDN=${FQDN}
	cat > presto-server-${PRESTO_VERSION}/etc/config.properties <<EOF
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=${HTTP_PORT}
query.max-memory=999TB
query.max-memory-per-node=${PRESTO_QUERY_NODE_MB}MB
resources.reserved-system-memory=${PRESTO_RESERVED_SYSTEM_MB}MB
discovery-server.enabled=true
discovery.uri=http://${PRESTO_MASTER_FQDN}:${HTTP_PORT}
query.max-history=1000
task.concurrency=${CORES_PER_INSTANCE}
EOF

	# Install cli
	$(wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar -O /usr/bin/presto)
	$(chmod a+x /usr/bin/presto)
else
	MASTER_NODE_NAME=$(hostname | sed 's/\(.*\)-s\?w-[a-z0-9]*.*/\1/g')
	PRESTO_MASTER_FQDN=${MASTER_NODE_NAME}-m.${DNSNAME}
	cat > presto-server-${PRESTO_VERSION}/etc/config.properties <<EOF
coordinator=false
http-server.http.port=${HTTP_PORT}
query.max-memory=999TB
query.max-memory-per-node=${PRESTO_QUERY_NODE_MB}MB
resources.reserved-system-memory=${PRESTO_RESERVED_SYSTEM_MB}MB
discovery.uri=http://${PRESTO_MASTER_FQDN}:${HTTP_PORT}
query.max-history=1000
task.concurrency=${CORES_PER_INSTANCE}
EOF
fi

cat > presto-server-${PRESTO_VERSION}/etc/catalog/memory.properties <<EOF
connector.name=memory
memory.max-data-per-node=10GB
EOF

# Start presto
presto-server-${PRESTO_VERSION}/bin/launcher start