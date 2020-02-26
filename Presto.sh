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
ROLE=$(curl -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/attributes/PrestoRole)
MASTER=$(curl -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/attributes/PrestoMaster)
HTTP_PORT="8080"
INSTANCE_VCPUS=32
INSTANCE_MEMORY=256000
TASK_CONCURRENCY=$(( ${INSTANCE_VCPUS} ))
PRESTO_JVM_MB=$(( ${INSTANCE_MEMORY} * 7 / 10 ))
QUERY_MAX_TOTAL_MEMORY_PER_NODE_MB=$(( ${PRESTO_JVM_MB} * 7 / 10 ))
QUERY_MAX_MEMORY_PER_NODE_MB=$(( ${QUERY_MAX_TOTAL_MEMORY_PER_NODE_MB} / 2 ))

# Prevents "Too many open files"
ulimit -n 30000

# Configure local ssd
for i in 1 2 3 4 5 6 7 8
do 
  mkfs.ext4 -F /dev/nvme0n$i
  mkdir -p /mnt/disks/ssd$i
  mount /dev/nvme0n$i /mnt/disks/ssd$i
  chmod a+w /mnt/disks/ssd$i
done

# Install Java
apt-get update
apt-get install openjdk-8-jre-headless -y
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/
echo 'export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/' >> /etc/bash.bashrc

# Install python, zip
apt-get install python zip -y

# Install Hadoop
wget -q http://mirrors.sonic.net/apache/hadoop/common/hadoop-2.10.0/hadoop-2.10.0.tar.gz
tar -zxf hadoop-2.10.0.tar.gz
mv hadoop-2.10.0 hadoop
export PATH=/hadoop/bin:$PATH
echo 'PATH=/hadoop/bin:$PATH' >> /etc/bash.bashrc

# Tell Hadoop where to store data
cat > /hadoop/etc/hadoop/hdfs-site.xml <<EOF
<configuration>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/mnt/disks/ssd1,/mnt/disks/ssd2,/mnt/disks/ssd3,/mnt/disks/ssd4,/mnt/disks/ssd5,/mnt/disks/ssd6,/mnt/disks/ssd7,/mnt/disks/ssd8</value>
    <description>Comma separated list of paths on the local filesystem of a DataNode where it should store its blocks.</description>
  </property>

  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/hadoop/dfs/name</value>
    <description>Path on the local filesystem where the NameNode stores the namespace and transaction logs persistently.</description>
  </property>

  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
EOF

cat > /hadoop/etc/hadoop/yarn-site.xml <<EOF
<?xml version="1.0"?>
<configuration>  
  <property>    
    <name>yarn.resourcemanager.hostname</name>    
    <value>${MASTER}</value>  
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <description>
      The maximum allocation for every container request at the RM, in
      terms of virtual CPU cores. Requests higher than this won't take
      effect, and will get capped to this value.
    </description>
    <name>yarn.scheduler.maximum-allocation-vcores</name>
    <value>32000</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>$(( ${INSTANCE_MEMORY} * 8 / 10 ))</value>
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>1024</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>${INSTANCE_VCPUS}</value>
    <description>
      Number of vcores that can be allocated for containers. This is used by
      the RM scheduler when allocating resources for containers. This is not
      used to limit the number of physical cores used by YARN containers.
    </description>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>$(( ${INSTANCE_MEMORY} * 8 / 10 ))</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration> 
EOF

cat > /hadoop/etc/hadoop/mapred-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>mapreduce.map.memory.mb</name>
    <value>1024</value>
  </property>
  <property>
    <name>mapreduce.reduce.memory.mb</name>
    <value>3072</value>
  </property>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
    <property>
    <name>mapreduce.reduce.java.opts</name>
    <value>-Xmx2457m</value>
  </property>
  <property>
    <name>mapreduce.map.java.opts</name>
    <value>-Xmx819m</value>
  </property>
  <property>
    <name>mapreduce.task.io.sort.mb</name>
    <value>256</value>
  </property>
</configuration>
EOF

# Tell Hadoop where the hdfs name node lives
cat > /hadoop/etc/hadoop/core-site.xml <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${MASTER}/</value>
    <description>NameNode URI</description>
  </property>

  <property>
    <name>fs.gs.metadata.cache.directory</name>
    <value>/hadoop_gcs_connector_metadata_cache</value>
    <description>
      Only used if fs.gs.metadata.cache.type is FILESYSTEM_BACKED, specifies
      the local path to use as the base path for storing mirrored GCS metadata.
      Must be an absolute path, must be a directory, and must be fully
      readable/writable/executable by any user running processes which use the
      GCS connector.
    </description>
  </property>
  <property>
    <name>fs.gs.impl</name>
    <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value>
    <description>The FileSystem for gs: (GCS) uris.</description>
  </property>
  <property>
    <name>fs.gs.project.id</name>
    <value>digital-arbor-400</value>
    <description>
      Google Cloud Project ID with access to configured GCS buckets.
    </description>
  </property>
    <property>
    <name>fs.gs.metadata.cache.enable</name>
    <value>false</value>
  </property>
  <property>
    <name>fs.gs.implicit.dir.infer.enable</name>
    <value>true</value>
    <description>
      If set, we create and return in-memory directory objects on the fly when
      no backing object exists, but we know there are files with the same
      prefix.
    </description>
  </property>
  <property>
    <name>fs.gs.application.name.suffix</name>
    <value>-hadoop</value>
    <description>
      Appended to the user-agent header for API requests to GCS to help identify
      the traffic as coming from Dataproc.
    </description>
  </property>
  <property>
    <name>fs.AbstractFileSystem.gs.impl</name>
    <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
    <description>The AbstractFileSystem for gs: (GCS) uris.</description>
  </property>
  <property>
    <name>fs.gs.metadata.cache.type</name>
    <value>FILESYSTEM_BACKED</value>
    <description>
      Specifies which implementation of DirectoryListCache to use for
      supplementing GCS API &amp;amp;quot;list&amp;amp;quot; requests. Supported
      implementations:       IN_MEMORY: Enforces immediate consistency within
      same Java process.       FILESYSTEM_BACKED: Enforces consistency across
      all cooperating processes       pointed at the same local mirror
      directory, which may be an NFS directory       for massively-distributed
      coordination.
    </description>
  </property>
  <property>
    <name>fs.gs.block.size</name>
    <value>134217728</value>
  </property>
</configuration>
EOF

# Install google cloud storage connector
wget -q https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar -O /hadoop/share/hadoop/common/lib/gcs-connector-latest-hadoop2.jar
mkdir /hadoop_gcs_connector_metadata_cache

if [[ "${ROLE}" == 'Master' ]]; then
## Start HDFS daemons
# Format the namenode directory (DO THIS ONLY ONCE, THE FIRST TIME)
/hadoop/bin/hdfs namenode -format
# Start the namenode daemon
/hadoop/sbin/hadoop-daemon.sh start namenode

## Start YARN daemons
# Start the resourcemanager daemon
/hadoop/sbin/yarn-daemon.sh start resourcemanager
fi

# Start the nodemanager daemon
/hadoop/sbin/yarn-daemon.sh start nodemanager
# Start the datanode daemon
/hadoop/sbin/hadoop-daemon.sh start datanode

# Install Hive
wget -q http://mirrors.sonic.net/apache/hive/hive-2.3.6/apache-hive-2.3.6-bin.tar.gz
tar -zxf apache-hive-2.3.6-bin.tar.gz
mv apache-hive-2.3.6-bin /hive
export PATH=/hive/bin:$PATH
echo 'PATH=/hive/bin:$PATH' >> /etc/bash.bashrc

# Configure hive
cat > /hive/conf/hive-site.xml <<EOF 
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>  
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://localhost/metastore</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hiveuser</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>mypassword</value>
  </property>

  <property>
    <name>datanucleus.autoCreateSchema</name>
    <value>false</value>
  </property>

  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://${MASTER}:9083</value>
    <description>IP address (or fully-qualified domain name) and port of the metastore host</description>
  </property>
</configuration>
EOF

if [[ "${ROLE}" == 'Master' ]]; then
hadoop fs -mkdir -p /tmp
hadoop fs -mkdir -p /user/hive/warehouse
hadoop fs -chmod a+w /tmp
hadoop fs -chmod a+w /user/hive/warehouse

# Install postgres for our metastore
apt-get install -y postgresql
service postgresql start
sudo -u postgres psql <<EOF
CREATE USER hiveuser WITH PASSWORD 'mypassword';
CREATE DATABASE metastore;
EOF

schematool -dbType postgres -initSchema

# Start hive metastore service
screen -S hive-metastore -d -m hive --service metastore
fi

# Download and unpack Presto server
wget -q https://repo1.maven.org/maven2/io/prestosql/presto-server/329/presto-server-329.tar.gz
tar -zxf presto-server-329.tar.gz
mv presto-server-329 presto

# Install cli
if [[ "${ROLE}" == 'Master' ]]; then
wget -q https://repo1.maven.org/maven2/io/prestosql/presto-cli/329/presto-cli-329-executable.jar -O /usr/bin/presto
chmod a+x /usr/bin/presto
fi

# Copy GCS connector
# TODO fiddle with caching options in https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/gcs/src/main/java/com/google/cloud/hadoop/fs/gcs/GoogleHadoopFileSystemBase.java
wget -q https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar -O presto/plugin/hive-hadoop2/gcs-connector-latest-hadoop2.jar 

# Configure Presto
mkdir -p presto/etc/catalog

cat > /presto/etc/node.properties <<EOF
node.environment=production
node.id=$(uuidgen)
node.data-dir=/var/presto/data
EOF

# Configure hive metastore
cat > /presto/etc/catalog/hive.properties <<EOF
connector.name=hive-hadoop2
hive.metastore.uri=thrift://${MASTER}:9083
hive.non-managed-table-writes-enabled=true
EOF

# Configure JVM
cat > /presto/etc/jvm.config <<EOF
-server
-Xmx${PRESTO_JVM_MB}m
-XX:-UseBiasedLocking
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+UseGCOverheadLimit
-XX:+HeapDumpOnOutOfMemoryError
-XX:ReservedCodeCacheSize=512M
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000

-Dhive.config.resources=/hadoop/etc/hadoop/core-site.xml
-Djava.library.path=/hadoop/lib/native/:/usr/lib/
EOF

if [[ "${ROLE}" == 'Master' ]]; then
	# Configure master properties
cat > /presto/etc/config.properties <<EOF
coordinator=true
node-scheduler.include-coordinator=true
discovery-server.enabled=true
http-server.http.port=${HTTP_PORT}
query.max-memory=99TB
query.max-total-memory-per-node=${QUERY_MAX_TOTAL_MEMORY_PER_NODE_MB}MB
query.max-memory-per-node=${QUERY_MAX_MEMORY_PER_NODE_MB}MB
discovery.uri=http://${MASTER}:${HTTP_PORT}
query.max-history=1000
task.concurrency=${TASK_CONCURRENCY}
EOF
else
cat > /presto/etc/config.properties <<EOF
coordinator=false
http-server.http.port=${HTTP_PORT}
query.max-memory=99TB
query.max-total-memory-per-node=${QUERY_MAX_TOTAL_MEMORY_PER_NODE_MB}MB
query.max-memory-per-node=${QUERY_MAX_MEMORY_PER_NODE_MB}MB
discovery.uri=http://${MASTER}:${HTTP_PORT}
query.max-history=1000
task.concurrency=${TASK_CONCURRENCY}
EOF
fi

# Start presto
presto/bin/launcher start