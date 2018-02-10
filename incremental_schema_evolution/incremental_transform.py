#!/usr/bin/env python

'''
~~~~~~~~~~~~~~~~~ This script is POC ~~~~~~~~~~~~~~


'''
import sys
# unless toolkit becomes package we have to use this method to load toolkit.py module
sys.path.append("/home/chrisrozacki/incremental/uc-schema-analyzer")
import toolkit
import config
import logging
import argparse
import subprocess

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mapping", action = "store", required = False, help="technical mapping", default=config.DELTA_MAPPING)
    parser.add_argument("-l", "--location", action = "store", required = False, help="location of ingested data folder", default=config.SOURCE_DELTA_FOLDER_LOCATION)
    parser.add_argument("-o", "--output", action = "store", required = False, help="intermediate transformation, sql output file", default=config.SQL_TRANFORM_SCRIPT)
    parser.add_argument("-p", "--partition", action = "store", required = False, help="partition date used to transform data", default=config.DELTA_PARTITION)
    parser.add_argument("-hl", "--hive_warehouse_data_location", action = "store", required = False, help="hive's data folder, managed data is copied from this managed location", default=config.HIVE_WAREHOUSE_DATA_LOCATION)
    parser.add_argument("-el", "--external_data_location", action = "store", required = False, help="external data folder, managed data is copied to this unmanaged location", default=config.EXTERNAL_DATA_LOCATION)
    parser.add_argument("-db", "--database_name", action = "store", required = False, help="source database name", default=config.DATABASE_NAME)
    parser.add_argument("-dr", "--dry_run", action = "store", required = False, help="source database name", default=False)
    parser.add_argument("-i", "--init", action = "store", required = False, help="init incremental load, if this option is enabled the source data location should point to full load", default=False)
    parser.add_argument("-ts", "--table_suffix", action = "store", required = False, help="suffix added to transient the tables to distinguish from the production table", default=config.DELTA_TABLE_SUFFIX)
    parser.add_argument("-tpm", "--transform_parameter_name", action = "store", required = False, help="what parameter name is use during transform, it's different for bulk na d delta", default=config.DELTA_TRANSFORMATION_PARAMETER_NAME)
    args = parser.parse_args()

    if args.init:
        logging.info("init incremental load on")
        args.table_suffix = config.BULK_TABLE_SUFFIX
        args.partition = config.BULK_INGEST_DAY
        args.location = config.SOURCE_BULK_FOLDER_LOCATION
        args.transform_parameter_name = config.BULK_TRANSFORMATION_PARAMETER_NAME
        args.mapping = config.BASE_MAPPING

    if args.dry_run:
        logging.info("dry run on")

    if not transform_tables(args):
        exit(1)

    if not update_table_partitions(args):
        exit(1)

def transform_tables(args):
    ''' Generate sql transformation script based on mapping and executes it. The outcome of this script may be many
    managed tables '''

    logging.info("generate sql script from mapping '{1}' to file '{0}' using location '{2}'".format(args.output, args.mapping, args.location))
    if not toolkit.generate_sql(args.mapping, args.output, args.location, None, "avro"):
        return False

    logging.info("transform data using {1}={0}".format(args.partition, args.transform_parameter_name))
    cmd = [
        "hive", "-f", args.output, "-hiveconf", "{1}={0}".format(args.partition, args.transform_parameter_name)
    ]
    logging.debug(cmd)
    if not args.dry_run:
        rc = subprocess.call(cmd)
        if rc:
            logging.error("error while calling hive cli")
            return False

    return True

def update_table_partitions(args):
    '''
    Copy data from managed folders (tables) to un-managed as partition. Refresh partition. Call update avro schema for each table
    '''

    logging.info("iterating all target tables and copy data from hive managed folder to un-managed partition folder")
    mapping  =  toolkit.read_mapping(args.mapping)
    for target_table_suffixed in toolkit.get_unique_by_column_id(mapping, toolkit.TARGET_TABLE):
        target_table_suffixed = target_table_suffixed.lower()
	logging.info("next target table '{0}'".format(target_table_suffixed))
        target_table_suffixed = target_table_suffixed.lower()
	target_table = target_table_suffixed
        if len(args.table_suffix) > 0:
            target_table = target_table_suffixed[ : - len(args.table_suffix)]
        source = "{0}/{1}.db/{2}/*".format(args.hive_warehouse_data_location, args.database_name, target_table_suffixed )
        location = "{0}/{1}/{2}".format(args.external_data_location, args.database_name, target_table)
        destination = "{0}/date_str={1}/".format(location, args.partition)

        target_file = config.HDFS_AVROSCHEMAS_LOCATION + "/"+ target_table + args.partition + ".avsc"
        if  not upload_table_avro_schema(
                args.hive_warehouse_data_location
                , target_file
                , args.database_name
                , target_table
                , target_table_suffixed
                , args.dry_run
                , args.init):
            return False

        logging.info("remove hdfs://{0} folder before copy".format(destination))
        cmd = [
            "hdfs", "dfs", "-rm", "-f", "-r", destination
        ]
        logging.debug(cmd)
        if not args.dry_run:
            rc = subprocess.call(cmd)
            if rc:
                logging.error("error while deleting folder")
                return False

        logging.info("create destination folder hdfs://{0}".format(destination))
        cmd = [
            "hdfs", "dfs", "-mkdir", "-p", destination
        ]
        logging.debug(cmd)
        if not args.dry_run:
            rc = subprocess.call(cmd)
            if rc:
                logging.error("error while creating folder")
                return False

        logging.info("copy data from hdfs://{0} to hdfs://{1}".format(source, destination))
        cmd = [
            "hdfs", "dfs", "-cp"
            , source
            , destination
        ]
        logging.debug(cmd)
        if not args.dry_run:
            rc = subprocess.call(cmd)
            if rc:
                logging.error("error while copying data")
                return False

        if args.init:
            logging.info("create external partitioned table '{0}'".format(target_table))

            create_sql =  "USE {3};DROP TABLE IF EXISTS {0};" \
                          "CREATE EXTERNAL TABLE {0}" \
                          " PARTITIONED BY (date_str string)" \
                          " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'" \
                          " STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'" \
                          " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'" \
                          " LOCATION '{1}'" \
                          " TBLPROPERTIES ('avro.schema.url'='hdfs://{2}')".format(target_table, location, target_file, args.database_name, args.database_name)

            cmd = [ "hive","-e", create_sql ]
            logging.debug(cmd)
            if not args.dry_run:
                rc = subprocess.call(cmd)
                if rc:
                    logging.error("error while creating hive table")
                    return False


        logging.info("repair table '{0}' to refresh partitions".format(target_table))
        cmd = [
            "hive", "-e", "use {0}; MSCK REPAIR TABLE {1};".format(args.database_name, target_table)
        ]
        logging.debug(cmd)
        if not args.dry_run:
            rc = subprocess.call(cmd)
            if rc:
                logging.error("error while calling hive cli")
                return False

        if not update_avro_table_schema(args.database_name, target_table
                , target_file
                , args.dry_run):
            return False

    return True

def upload_table_avro_schema(hive_warehouse_data_location, target_file, database, table, target_table_suffixed, dry_run, init):
    ''' Extracts avro schema from located files and uploads it into hdfs folder.
    The avro schema in HDFS folder will be used to SET new schema to. '''

    local_avro_data_file = "data.avro"

    source = "{0}/{1}.db/{2}/{3}".format(hive_warehouse_data_location, database, target_table_suffixed,"000000_0")
    logging.info("extract avro schema from hdfs://{0} and upload to hdfs://{1}".format(source, target_file))

    logging.info("remove local file '{0}' if exists".format(local_avro_data_file))
    if not dry_run:
        toolkit.silentremove(local_avro_data_file)

    logging.info("copy hdfs://{0} to local '{1}'".format(source, local_avro_data_file))
    cmd = ['hdfs', 'dfs', '-get', source, local_avro_data_file]
    logging.debug(cmd)
    if not dry_run:
        rc = subprocess.call(cmd)
        if rc:
            logging.error("error while getting hdfs file")
            return False

    '''
    if not dry_run:
        rc = subprocess.Popen(cmd, stdout = subprocess.PIPE)
        content, err = rc.communicate()
        if err:
            logging.error("error while getting hdfs file " + err)
            return False
    
    logging.info("store avro file as '{0}'".format(local_avro_data_file))
    if not dry_run:
        with open(local_avro_data_file,"w") as file:
            file.write(content)
    '''

    logging.info("use avro-tools to get the schema")
    cmd = ["java", "-jar", config.AVRO_TOOLS_LOCATION, "getschema", "data.avro"]
    logging.debug(cmd)
    if not dry_run:
        rc = subprocess.Popen(cmd, stdout = subprocess.PIPE)
        content, err = rc.communicate()
        if err:
            logging.error("error while getting schema " + err)
            return False

    logging.info("store avro schema in 'data.avsc'")
    if not dry_run:
        with open("data.avsc", "w") as file:
            file.write(content)

    logging.info("upload schema to hdfs://{0}".format(target_file))
    cmd = ['hdfs', 'dfs', '-copyFromLocal', '-f', 'data.avsc', target_file]
    logging.debug(cmd)
    if not dry_run:
        rc = subprocess.call(cmd)
        if rc:
            logging.error("error while copying to hdfs")
            return False

    return True

def update_avro_table_schema(database, table, target_file, dry_run):
    logging.info("change table '{0}' avro schema to '{1}'".format(table, target_file))
    alter_schema_sql = "use {0}; alter table {1} set tblproperties('avro.schema.url'='hdfs://{2}')".format(database, table, target_file)
    cmd = ['hive', '-e', alter_schema_sql]
    logging.debug(cmd)
    if not dry_run:
        rc = subprocess.call(cmd)
        if rc:
            logging.error("error while altering table schema")
            return False

    return True

if __name__=="__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout,level=logging.INFO, format='%(asctime)s %(levelname)s %(module)s: %(message)s')
    main()
