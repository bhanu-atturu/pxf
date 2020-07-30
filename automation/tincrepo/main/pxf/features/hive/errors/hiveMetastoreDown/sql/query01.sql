-- @description query01 for PXF Hive feature checking error when Hive metastore is down.

-- start_matchsubs
--                                                                                               
-- # create a match/subs
--
-- m/(ERROR|WARNING):.*remote component error.*\(\d+\).*from.*'\d+\.\d+\.\d+\.\d+:\d+'.*/
-- s/'\d+\.\d+\.\d+\.\d+:\d+'/'SOME_IP:SOME_PORT'/
--
-- m/Exception (r|R)eport.*(Failed connecting to Hive MetaStore service: Could not connect to meta store using any of the URIs provided. Most recent failure: org.apache.thrift.transport.TTransportException: java.net.ConnectException: Connection refused).*/
-- s/(r|R)eport.*/Exception Report   Failed connecting to Hive MetaStore service: Could not connect to meta store using any of the URIs provided. Most recent failure: org.apache.thrift.transport.TTransportException: java.net.ConnectException: Connection refused/
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- end_matchsubs
SELECT *  FROM pxf_hive_metastore_down;