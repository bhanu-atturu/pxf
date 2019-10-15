package org.greenplum.pxf.plugins.hdfs.utilities;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.greenplum.pxf.api.io.DataType.UNSUPPORTED_TYPE;
import static org.greenplum.pxf.api.io.DataType.isArrayType;

public class AvroUtilities {
    private static String COMMON_NAMESPACE = "public.avro";

    private static Schema readOrGenerateAvroSchema(RequestContext context, Configuration configuration) throws IOException {
        String userProvidedSchemaFile = context.getOption("SCHEMA");
        // user-provided schema trumps everything
        if (userProvidedSchemaFile != null) {
            if (userProvidedSchemaFile.matches("^.*\\.avsc$")) {
                try (InputStream externalSchema = getJsonSchemaStream(configuration, userProvidedSchemaFile)) {
                    return (new Schema.Parser()).parse(externalSchema);
                }
            }
            return readAvroSchema(configuration, userProvidedSchemaFile);
        }

        // if we are writing we must generate the schema if there is none to read
        if (context.getRequestType() == RequestContext.RequestType.WRITE_BRIDGE) {
            return generateAvroSchema(context.getTupleDescription());
        }

        // reading from external: get the schema from data source
        return readAvroSchema(configuration, context.getDataSource());
    }

    private static InputStream getJsonSchemaStream(Configuration configuration, String schemaName) throws IOException {

        // search HDFS first
        FileSystem fs = FileSystem.get(configuration);
        Path path = new Path(schemaName);
        if (fs.exists(path)) {
            return new FSDataInputStream(fs.open(path));
        }

        // search full-path next
        File file = new File(schemaName);
        if (file.exists()) {
            return new FileInputStream(file);
        }

        // check on classpath last
        ClassLoader loader = AvroUtilities.class.getClassLoader();

        /** Testing that the schema resource exists. */
        if (loader.getResource(schemaName) == null) {
            throw new DataSchemaException(DataSchemaException.MessageFmt.SCHEMA_NOT_ON_CLASSPATH, schemaName);
        }
        return loader.getResourceAsStream(schemaName);
    }

    /**
     * Accessing the Avro file through the "unsplittable" API just to get the
     * schema. The splittable API (AvroInputFormat) which is the one we will be
     * using to fetch the records, does not support getting the Avro schema yet.
     *
     * @param conf       Hadoop configuration
     * @param dataSource Avro file (i.e fileName.avro) path
     * @return the Avro schema
     * @throws IOException if I/O error occurred while accessing Avro schema file
     */
    private static Schema readAvroSchema(Configuration conf, String dataSource) throws IOException {
        final Path path = new Path(dataSource);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> fileReader = null;

        try {
            // check inside HDFS first
            if (FileSystem.get(conf).exists(path)) {
                FsInput inStream = new FsInput(path, conf);
                fileReader = new DataFileReader<>(inStream, datumReader);
                return fileReader.getSchema();
            }

            /*
             * if user provided a full path, use that.
             * otherwise we need to check classpath
            */
            File file = new File(dataSource);
            if (!file.exists()) {
                ClassLoader loader = AvroUtilities.class.getClassLoader();

                /** Testing that the schema resource exists. */
                if (loader.getResource(dataSource) == null) {
                    throw new DataSchemaException(DataSchemaException.MessageFmt.SCHEMA_NOT_ON_CLASSPATH, dataSource);
                }
                file = new File(loader.getResource(dataSource).getPath());
            }

            fileReader = new DataFileReader<>(file, datumReader);
            return fileReader.getSchema();
        } finally {
            if (fileReader != null) {
                fileReader.close();
            }
        }
    }

    private static Schema generateAvroSchema(List<ColumnDescriptor> tupleDescription) throws IOException {
        String colName;
        int colType;

        Schema schema = Schema.createRecord("tableName", "", COMMON_NAMESPACE, false);
        List<Schema.Field> fields = new ArrayList<>();

        for (ColumnDescriptor cd : tupleDescription) {
            colName = cd.columnName();
            colType = cd.columnTypeCode();
            // String delim = context.getOption("delimiter");
            // columnDelimiter = delim == null ? ',' : delim.charAt(0);
            fields.add(new Schema.Field(colName, getFieldSchema(DataType.get(colType), false, 1), "", null));
        }

        schema.setFields(fields);

        return schema;
    }

    private static Schema getFieldSchema(DataType type, boolean notNull, int dim) throws IOException {
        List<Schema> unionList = new ArrayList<>();
        // in this version of gpdb, external table should not set 'notnull' attribute
        unionList.add(Schema.create(Schema.Type.NULL));

        switch (type) {
            case BOOLEAN:
                unionList.add(Schema.create(Schema.Type.BOOLEAN));
                break;
            case BYTEA:
                unionList.add(Schema.create(Schema.Type.BYTES));
                break;
            case BIGINT:
                unionList.add(Schema.create(Schema.Type.LONG));
                break;
            case SMALLINT:
            case INTEGER:
                unionList.add(Schema.create(Schema.Type.INT));
                break;
            case REAL:
                unionList.add(Schema.create(Schema.Type.FLOAT));
                break;
            case FLOAT8:
                unionList.add(Schema.create(Schema.Type.DOUBLE));
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                break;
            case VARCHAR:
            case BPCHAR:
            case NUMERIC:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TEXT:
                unionList.add(Schema.create(Schema.Type.STRING));
                break;
            case INT2ARRAY:
            case INT4ARRAY:
            case INT8ARRAY:
            case BOOLARRAY:
            case TEXTARRAY:
            default:
                if (type == UNSUPPORTED_TYPE) {
                    throw new UnsupportedTypeException("Unsupported type");
                }
                if (!isArrayType(type.getOID())) {
                    unionList.add(Schema.create(Schema.Type.STRING));
                    break;
                }
                // array or other variable length types
                DataType elementType = type.getTypeElem();
                Schema array = Schema.createArray(getFieldSchema(elementType, notNull, 0));
                // for multi-dim array
                for (int i = 1; i < dim; i++) {
                    array = Schema.createArray(array);
                }
                unionList.add(array);

                break;
        }

        return Schema.createUnion(unionList);
    }

    /**
     * All-purpose method for obtaining an Avro schema based on the request context and
     * HDFS config.
     *
     * @param context
     * @param configuration
     * @return
     */
    public static Schema obtainSchema(RequestContext context, Configuration configuration) {
        Schema schema = (Schema) context.getMetadata();

        if (schema != null) {
            return schema;
        }
        try {
            schema = readOrGenerateAvroSchema(context, configuration);
        } catch (IOException e) {
            throw new RuntimeException("Failed to obtain Avro schema for " + context.getDataSource(), e);
        }
        context.setMetadata(schema);
        return schema;
    }
}
