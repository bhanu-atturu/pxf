SingleCluster-HDP3
=============

Single cluster is a self contained, easy to deploy distribution of HDP3 
It contains the following versions: 
- Hadoop 3
- Hive 3.1

This version of Single cluster requires users to make some manual changes to the configuration files once the 


Initialization
--------------

1. Untar the singlecluster tarball
```sh
mv singlecluster.tar.gz ~/.
cd ~/.
tar -xzvf singlecluster-HDP3.tar.gz
cd singlecluster-HDP3
export GPHD_ROOT=${PWD}
```

2. In `${GPHD_ROOT}/hive/conf/hive-env.sh`, remove `-hiveconf hive.log.dir=$LOGS_ROOT` from the `HIVE_OPTS` and `HIVE_SERVER_OPTS` exports: 

```sh
export HIVE_OPTS="-hiveconf derby.stream.error.file=$LOGS_ROOT/derby.log -hiveconf javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$HIVE_STORAGE_ROOT/metastore_db;create=true"
export HIVE_SERVER_OPTS="-hiveconf derby.stream.error.file=$LOGS_ROOT/derby.log -hiveconf ;databaseName=$HIVE_STORAGE_ROOT/metastore_db;create=true"
```

3. Update the `hive.execution.engine` property to `tez` in `${GPHD_ROOT}/hive/conf/hive-site.xml`:
```xml
	<property>
		<name>hive.execution.engine</name>
		<value>tez</value>
		<description>Chooses execution engine. Options are: mr(default), tez, or spark</description>
	</property>
```

4. Add the following properties to `${GPHD_ROOT}/hive/conf/hive-site.xml`: 
```xml
	<property>
		<name>hive.tez.container.size</name>
		<value>2048</value>
	</property>
	<property>
		<name>datanucleus.schema.autoCreateAll</name>
		<value>True</value>
	</property>
	<property>
		<name>metastore.metastore.event.db.notification.api.auth</name>
		<value>false</value>
	</property>
```

5. Add the following property to `"${GPHD_ROOT}/tez/conf/tez-site.xml`:
```xml
		<property>
			<name>tez.use.cluster.hadoop-libs</name>
			<value>true</value>
		</property>
```

6. Update the properties in the `${GPHD_ROOT}/hadoop/etc/hadoop/yarn-site.xml` by replacing `HADOOP_CONF` with `HADOOP_CONF_DIR` and `HADOOP_ROOT` with `HADOOP_HOME`:
```sh
sed -i -e 's|HADOOP_CONF|HADOOP_CONF_DIR|g' \
	   -e 's|HADOOP_ROOT|HADOOP_HOME|g' "${GPHD_ROOT}/hadoop/etc/hadoop/yarn-site.xml"
```

7. Initialize an instance
```sh
${GPHD_ROOT}/bin/init-gphd.sh
```

8. Add the following to your environment
```sh
export HADOOP_ROOT=$GPHD_ROOT/hadoop
export HBASE_ROOT=$GPHD_ROOT/hbase
export HIVE_ROOT=$GPHD_ROOT/hive
export ZOOKEEPER_ROOT=$GPHD_ROOT/zookeeper
export PATH=$PATH:$GPHD_ROOT/bin:$HADOOP_ROOT/bin:$HBASE_ROOT/bin:$HIVE_ROOT/bin:$ZOOKEEPER_ROOT/bin
```


Usage
-----

-	Start all Hadoop services
	-	$GPHD_ROOT/bin/start-gphd.sh
-	Start HDFS only
	-	$GPHD_ROOT/bin/start-hdfs.sh
-	Start PXF only (Install pxf first to make this work. [See Install PXF session here](https://cwiki.apache.org/confluence/display/HAWQ/PXF+Build+and+Install))
	-	$GPHD_ROOT/bin/start-pxf.sh
-	Start HBase only (requires hdfs and zookeeper)
	-	$GPHD_ROOT/bin/start-hbase.sh
-	Start ZooKeeper only
	-	$GPHD_ROOT/bin/start-zookeeper.sh
-	Start YARN only
	-	$GPHD_ROOT/bin/start-yarn.sh
-	Start Hive (MetaStore)
	-	$GPHD_ROOT/bin/start-hive.sh
- 	Stop all PHD services
	- 	$GPHD_ROOT/bin/stop-gphd.sh
-	Stop an individual component
	-	$GPHD_ROOT/bin/stop-[hdfs|pxf|hbase|zookeeper|yarn|hive].sh
-	Start/stop HiveServer2
	-	$GPHD_ROOT/bin/hive-service.sh hiveserver2 start
	-	$GPHD_ROOT/bin/hive-service.sh hiveserver2 stop

Notes
-----

1.	Make sure you have enough memory and space to run all services. Typically about 24GB space is needed to run pxf automation.
2.	All of the data is stored under $GPHD_ROOT/storage. Cleanup this directory before running init again.


For Hive
--------
When you run `./hive`, it uses beeline. You can then run 
```shell
!connect jdbc:hive2://localhost:10000/default
```
with no username and no password. 

If you receive the following error, give the system a minute or two to finish spinning up the hiveserver before trying again. 
```shell
WARN jdbc.HiveConnection: Failed to connect to localhost:10000
Could not open connection to the HS2 server. Please check the server URI and if the URI is correct, then ask the administrator to check the server status.
Error: Could not open client transport with JDBC Uri: jdbc:hive2://localhost:10000/default: java.net.ConnectException: Connection refused (Connection refused) (state=08S01,code=0)
```
You can also check using netstat to see when the server has finished spinning up: 
```
netstat -vanp tcp | grep 10000
```