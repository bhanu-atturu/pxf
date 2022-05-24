package org.greenplum.pxf.automation.features.orc;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.io.File;

public class OrcWriteTest extends BaseFeature {

    private static final String ORC_PRIMITIVE_TYPES = "orc_types.orc";
    private static final String PXF_ORC_TABLE = "pxf_orc_primitive_types_writable";
    private static final String ORC_PRIMITIVE_TYPES_UNORDERED_SUBSET = "orc_types_unordered_subset.orc";

    private static final String[] ORC_TABLE_COLUMNS = {
            "id      integer",
            "name    text",
            "cdate   date",
            "amt     double precision",
            "grade   text",
            "b       boolean",
            "tm      timestamp without time zone",
            "bg      bigint",
            "bin     bytea",
            "sml     smallint",
            "r       real",
            "vc1     character varying(5)",
            "c1      character(3)",
            "dec1    numeric",
            "dec2    numeric(5,2)",
            "dec3    numeric(13,5)",
            "num1    integer"
    };

    private static final String[] ORC_PRIMITIVE_TABLE_COLS = new String[]{
            "type_int         int",
            "type_smallint    smallint",
            "type_long        bigint",
            "type_float       real",
            "type_double      float8",
            "type_string      text",
            "type_bytes       bytea",
            "type_boolean     bool",
            "type_char        character(20)",
            "type_varchar     varchar(32)"
    };

    private String gpdbTable;
    private String hdfsPath;
    private String resourcePath;
    private String fullTestPath;
    private ProtocolEnum protocol;

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/writableOrc/";
        protocol = ProtocolUtils.getProtocol();

        resourcePath = localDataResourcesFolder + "/orc/";
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWritePrimitives() throws Exception {
        gpdbTable = "orc_primitive_types";
        fullTestPath = hdfsPath + "writable_orc_primitive_types";
        prepareWritableExternalTable(gpdbTable, ORC_PRIMITIVE_TABLE_COLS, fullTestPath);

        prepareReadableExternalTable(gpdbTable, ORC_PRIMITIVE_TABLE_COLS, fullTestPath, false /*mayByPosition*/);

        insertPrimitives(gpdbTable);
        runTincTest("pxf.features.orc.write.primitive_types.runTest");
    }

    private void insertPrimitives(String exTable) throws Exception {
        gpdb.runQuery("INSERT INTO " + exTable + "_writable " + "SELECT " +
                "i, " +                                             // type_int
                "i, " +                                             // type_smallint
                "i*100000000000, " +                                // type_long
                "i+1.0001, " +                                      // type_float
                "i*100000.0001, " +                                 // type_double
                "'row_' || i::varchar(255), " +                     // type_string
                "('bytes for ' || i::varchar(255))::bytea, " +      // type_bytes
                "CASE WHEN (i%2) = 0 THEN TRUE ELSE FALSE END, " +  // type_boolean
                "'character row ' || i::char(3)," +                 // type_character
                "'character varying row ' || i::varchar(3)" +       // type_varchar
                "from generate_series(1, 100) s(i);");
    }

    private void prepareWritableExternalTable(String name, String[] fields, String path) throws Exception {
        exTable = new WritableExternalTable(name + "_writable", fields,
                protocol.getExternalTablePath(hdfs.getBasePath(), path), "custom");
        exTable.setFormatter("pxfwritable_export");
        exTable.setProfile(protocol.value() + ":orc");

        createTable(exTable);
    }

    private void prepareReadableExternalTable(String name, String[] fields, String path, boolean mapByPosition) throws Exception {
        exTable = new ReadableExternalTable(name+ "_readable", fields,
                protocol.getExternalTablePath(hdfs.getBasePath(), path), "custom");
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(protocol.value() + ":orc");

        if (mapByPosition) {
            exTable.setUserParameters(new String[]{"MAP_BY_POSITION=true"});
        }

        createTable(exTable);
    }
}
