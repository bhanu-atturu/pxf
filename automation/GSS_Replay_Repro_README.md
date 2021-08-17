Steps for attempting to reproduce GSS initiate failure with Kerberos
Hijack into the appropriate container and select the Test No Impersonation Job:
```
fly hijack -u https://ud.ci.gpdb.pivotal.io/teams/main/pipelines/dev:pandeyhi-kerb-replay-repro/jobs/<Test Multi-No-Impers-HDP2-RHEL7 Job>/build/<build #>
``` 

Set up the environment by setting JAVA_HOME for hadoop/hadoop related tasks and sourcing greenplum path
```
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
source /usr/local/greenplum-db/greenplum_path.sh
```

Set up Kerberos with Hadoop
Modify `/singlecluster/hadoop/etc/hadoop/conf/core-site.xml` and add the following properties:

```
  <property>
    <name>hadoop.security.authentication</name>
    <value>kerberos</value>
  </property>
  <property>
    <name>hadoop.security.authorization</name>
    <value>true</value>
  </property>
  <property>
    <name>hadoop.rpc.protection</name>
    <value>privacy</value>
  </property>
```

Createa a kerberos ticket for root using the principal
```
kinit -kt /etc/security/keytabs/gpadmin.headless.keytab gpadmin@C.DATA-GPDB-UD.INTERNAL
```

Check if the ticket is there
```
klist
```

Set up gsutil to pull down data from GCP: 
```
curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-352.0.0-linux-x86_64.tar.gz
tar -xvzf google-cloud-sdk-352.0.0-linux-x86_64.tar.gz
./google-cloud-sdk/install.sh
./google-cloud-sdk/bin/gcloud init
```

gsutil requires `crcmod` to download files from GCP:
```
pip install crcmod
```

Pull down TPC-H lineitem table data from GCP:
```
gsutil -m cp -r gs://data-gpdb-ud-tpch/1/lineitem_data /tmp/build/***/
```

Modify the above datafile to have fewer rows
The goal is to have a lot of fragments so run the following script to produce at least 100 files. For eg: 
```
cd /tmp/build/***/lineitem_data
for i in {1..100}; do cp lineitem1.tbl "lineitem$i.tbl"; done
```

Push the data files to the hadoop clusters:
```
/singlecluster/bin/hdfs dfs -mkdir /tpch/lineitem
/singlecluster/bin/hdfs dfs -copyFromLocal /tmp/build/***/lineitem_data /tpch/lineitem
```

Create the table in psql:
```
 CREATE EXTERNAL TABLE test_lineitem (
    l_orderkey    BIGINT,
    l_partkey     BIGINT,
    l_suppkey     BIGINT,
    l_linenumber  BIGINT,
    l_quantity    DECIMAL(15,2),
    l_extendedprice  DECIMAL(15,2),
    l_discount    DECIMAL(15,2),
    l_tax         DECIMAL(15,2),
    l_returnflag  CHAR(1),
    l_linestatus  CHAR(1),
    l_shipdate    DATE,
    l_commitdate  DATE,
    l_receiptdate DATE,
    l_shipinstruct CHAR(25),
    l_shipmode     CHAR(10),
    l_comment VARCHAR(44)
) LOCATION ('pxf://tpch/lineitem?PROFILE=hdfs:text') FORMAT 'CSV' (DELIMITER='|');
```

Create a shell script `lineitem_query.sh` and add the following select at least 100 times: 
```
psql -c 'select now(),count(*) from test_lineitem;'
```

Give the proper access to the file:
```
chmod +x lineitem_query.sh
```

Run the above shell script in nohup mode: 
```
nohup ./lineitem_query.sh &
nohup ./lineitem_query.sh &
nohup ./lineitem_query.sh &
nohup ./lineitem_query.sh &
nohup ./lineitem_query.sh &
nohup ./lineitem_query.sh &
nohup ./lineitem_query.sh &
nohup ./lineitem_query.sh &
nohup ./lineitem_query.sh &
nohup ./lineitem_query.sh &
nohup ./lineitem_query.sh &
```
