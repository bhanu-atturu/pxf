package org.greenplum.pxf.automation.smoke;

import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

public class SystemdSmoke extends BaseSmoke {
    @Override
    protected void prepareData() throws Exception {
        Table dataTable = getSmallData();
        hdfs.writeTableToFile(hdfs.getWorkingDirectory() + "/" + fileName, dataTable, ",");
    }

    @Override
    protected void createTables() throws Exception {
        ReadableExternalTable externalTable = TableFactory.getPxfReadableTextTable("pxf_smoke_small_data", new String[]{
                "name text",
                "num integer",
                "dub double precision",
                "longNum bigint",
                "bool boolean"
        }, hdfs.getWorkingDirectory() + "/" + fileName, ",");
        externalTable.setHost(pxfHost);
        externalTable.setPort(pxfPort);
        gpdb.createTableAndVerify(externalTable);
    }

    @Override
    protected void queryResults() throws Exception {
        runTincTest("pxf.smoke.small_data.runTest");
    }

    @Test(groups = "systemd")
    public void testRestart() throws Exception {
        prepareData();
        createTables();
        queryResults();
        shutdownPxf();
        queryResults();
    }

    private void shutdownPxf() throws Exception {
        cluster.runCommand("curl -X POST localhost:5888/actuator/shutdown");
        Thread.sleep(10_000L);

//        int attempts = 10;
//        do {
//            attempts--;
//            cluster.runCommand("curl localhost:5888/actuator/health");
//            if (cluster.getLastCommandExitCode() == 0) {
//                break;
//            }
//            Thread.sleep(1_000L);
//        } while (attempts > 0);
    }
}
