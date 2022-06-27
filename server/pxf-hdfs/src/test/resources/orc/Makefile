# default assumes ORC tools JAR is downloaded in current directory
# download from https://search.maven.org/artifact/org.apache.orc/orc-tools/1.7.5/jar
ORC_TOOLS_JAR ?= orc-tools-1.7.5-uber.jar

# the CSV data files use '\N' for null columns but the CSV parsing in orc-tools discards '\'
# therefore the proper null string for orc-tools convert is 'N'
CSV_NULL = N

.PHONY: all
all: orc_types.orc orc_types_compound.orc

orc_types.orc: orc_types.json
	java -jar $(ORC_TOOLS_JAR) convert \
		--schema 'struct<t1:string,t2:string,num1:int,dub1:double,dec1:decimal(38,18),tm:timestamp,r:float,bg:bigint,b:boolean,tn:tinyint,sml:smallint,dt:date,vc1:varchar(5),c1:char(3),bin:binary>' \
		--output orc_types.orc \
		--overwrite \
		orc_types.json

orc_types_compound.orc: orc_types_compound.json
	java -jar $(ORC_TOOLS_JAR) convert \
    		--schema 'struct<id:int,bool_arr:array<boolean>,int2_arr:array<smallint>,int_arr:array<int>,int8_arr:array<bigint>,float_arr:array<float>,float8_arr:array<double>,text_arr:array<string>,bytea_arr:array<binary>,char_arr:array<char(15)>,varchar_arr:array<varchar(15)>>' \
    		--output orc_types_compound.orc \
    		--overwrite \
    		orc_types_compound.json