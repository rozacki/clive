#!/usr/bin/env python
import subprocess
import argparse
import datetime
import psycopg2
import logging
import sys
import imp
import os.path

# To transform single collection:
# ./generate_tables.py -c accepted-data/otherBenefit --refresh-schema -y ../test/chrisrozacki/python/chrisrozacki.yaml

# to transform all ingested collections
# ./generate_tables.py -f collections_list.py --refresh-schema -y canonical.yaml -w /var/tmp/relational-layer

# global flag to when transforming many collections ignore errors and carry on
ignore_errors = False

def main():
    global ignore_errors

    parser = argparse.ArgumentParser()
    parser.add_argument("-y", "--config-file", action = "store", required = False)
    parser.add_argument("-d", "--date", action = "store", required = False, default = datetime.date.today().isoformat(), help = "date YYYY-MM-DD")
    parser.add_argument("-w", "--working-folder", action = "store", required = False, default = "/var/tmp/relational-model", help = "working folder, must be absolute")

    # either single db/collection or file
    file_or_collection_feature_group = parser.add_mutually_exclusive_group(required=True)
    file_or_collection_feature_group.add_argument("-c", "--collection", action = "store", required = False, help = "collection name in format db/collection")
    file_or_collection_feature_group.add_argument("-f", "--collections_file", action = "store", required = False, help = "json file containing databases and collections")
    #
    ignore_errors_feature_group = parser.add_mutually_exclusive_group(required=False)
    ignore_errors_feature_group.add_argument("-i", "--ignore-errors", action = "store_true", required = False, help = "when transforming many collections ignore errors and carry on")
    #
    refresh_feature_group = parser.add_mutually_exclusive_group(required=False)
    refresh_feature_group.add_argument("-r", "--refresh", action = "store_false", required = False, default=False, help = "force refreshing transformation files (json, csv, sql) in the working folder)")
    args = parser.parse_args()

    # where all transformations are kept
    working_folder = args.working_folder
    if not os.path.exists(working_folder):
        os.makedirs(working_folder)

    ignore_errors = args.ignore_errors
    logging.debug("working folder " + working_folder)
    logging.debug("ignore error is " + str(ignore_errors))
    logging.debug("refresh schema is " + str(args.refresh))

    working_folder +='/'

    # transform single collection
    if args.collection is not None:
        rc = generate(args.config_file, args.refresh, args.collection, args.date, working_folder)
        #todo: if rc is false then exit(1)?
        exit(0)

    # iterate and transform collections
    if args.collections_file is not None:
        try:
            collections_file = imp.load_source('collections_file', args.collections_file)
        except Exception as e:
            logging.error("error while reading the collections file " + str(e))
            exit(1)
        for db in collections_file.collections_by_dbs:
            for collection in collections_file.collections_by_dbs[db]:
                collection_name = collection
                collection_name = db + "/" + collection_name
                generate(args.config_file, args.refresh, collection_name, args.date, working_folder)


'''
Transforms single collection into relational layer
'''
def generate(config_file, refresh, collection, date, working_folder):
    logging.info("generate collection %s, date %s" % (collection, date))

    #load helping library and configuration
    dwbatch = imp.load_source("dwbatch","../../etl/python/dwbatch.py")
    # read configuration
    cfg = dwbatch.read_config(config_file)

    schema_file_name, refresh = get_schema(collection, date, working_folder, refresh)

    mapping_file, refresh = get_mapping(cfg, collection, date, schema_file_name, refresh)

    sql_file, refresh = get_sql(mapping_file, date, refresh, cfg["run"]["hive_database"])

    return run_sql(sql_file)

''' Generates schema from data.
    returns collection schema file name if we want to reuse the existing one
    it can be helpful when schema generation takes to long
'''
def get_schema(collection, date, working_folder, refresh):
    schema_file_name = working_folder + collection.replace("/","_") + ".json"
    schema_file_name_bak = working_folder + collection.replace("/","_") + ".json.bak"

    if os.path.isfile(schema_file_name) == True and refresh == False:
        logging.info("using existing schema %s" % (schema_file_name))
        return (schema_file_name, False)
    logging.info("generate schema for collection %s, date %s" % (collection, date))


    # submit spark job to generate schema
    # spark-submit get_json_schema.py -c agent-core/agent

    logging.info("generate json schema " + schema_file_name_bak)
    cmd = [
        'spark-submit', '/var/tmp/get_json_schema.py', '-c', collection, '-d', date
    ]

    with open(schema_file_name_bak,"w") as schemaFile:
        rc = subprocess.call(cmd, stdout=schemaFile)
        # we swollow as currently get_json_schema returns 0 but spark returns rc<>0
        #if rc:
        #    dwbatch.die(1,"error while generating schema %s/%s" % (date, collection))

    # cut off 2 first lines
    cmd =[
        'tail', '-n', '+3', schema_file_name_bak
    ]

    with open(schema_file_name,"w") as schemaFile:
        rc = subprocess.call(cmd, stdout=schemaFile)
        if rc:
            die(1,"error while removing two first lines from the schema file %s/%s" % (date, collection))

    return (schema_file_name, True)

'''
generates technical mapping using postgres
'''
def get_mapping(cfg, collection, date, schema_file_name, refresh):

    mapping_file_name= schema_file_name.replace("json","csv")
    if os.path.isfile(mapping_file_name) == True and refresh == False:
        logging.info("using existing mapping %s" % (mapping_file_name))
        return (mapping_file_name, False)
    logging.info("generate mapping for collection %s, date %s" % (collection, date))

    source_database_name = collection.split('/')[0]
    source_collection_name = collection.split('/')[1]

    # load schema to postgres and call the script to generate mapping
    logging.debug("opening postgres connection")

    try:
        # open postgres connection
        postgres =  psycopg2.connect(host = cfg["run"]["pg_hostname"]
                                     ,database = cfg["run"]["pg_dbname"]
                                     ,user = cfg["run"]["pg_username"]
                                     ,password = cfg["run"]["pg_password"])
    except Exception as e:
        die(1, "error while connecting to the postgres database " + str(e))

    pg_cur = postgres.cursor()
    try:
        pg_cur.execute("COPY (SELECT etl.get_mappings(%s, %s, %s, %s)) To %s WITH CSV DELIMITER '|';"
                       , (source_database_name, source_collection_name, schema_file_name, source_collection_name, mapping_file_name))
        postgres.commit()
    except Exception as e:
        logging.error("error while generating mapping " + str(e))
        exit(1)

    return (mapping_file_name, True)

'''
generates sql using JIVE
'''
def get_sql(mapping_file_name, date, refresh, database):

    sql_file_name = mapping_file_name.replace("csv","sql")
    if os.path.isfile(sql_file_name) == True and refresh == False:
        logging.info("using existing mapping %s" % (sql_file_name))
        return (sql_file_name, False)
    logging.info("generate sql for mapping file %s" % (mapping_file_name))

    # call jive to generate SQL
    cmd = [
        'java', '-cp', 'jive.jar', 'uk.gov.dwp.uc.dip.schemagenerator.SchemaGenerator'
        , '-tm', mapping_file_name, '-l', '/etl/uc/mongo/'+date, "-orc"
    ]

    with open(sql_file_name,"w") as sqlFile:
        rc = subprocess.call(cmd, stdout=sqlFile)
        if rc:
            die(1,"error while generating sql transformation scripts %s" % (mapping_file_name))

    with file(sql_file_name, 'r') as original: data = original.read()
    # hardcoded size of container, 3 * yarn.scheduler.minimum-allocation-mb
    with file(sql_file_name, 'w') as modified: modified.write(("SET hive.tez.container.size=16896;\nUSE %s;\n" % (database))+ data )

    logging.info("generated sql transform file " + sql_file_name)

    return (sql_file_name, True)

'''
Runs sql
'''
def run_sql(sql_file_name):
    logging.debug("sql transformation file %s" % (sql_file_name))

    # hive -f ...
    logging.info("opening hive session")
    cmd = [
        'hive','-f', sql_file_name
    ]
    rc = subprocess.call(cmd)
    if rc:
        die(1,"error while executing sql %s" % (sql_file_name))

    # rbac - static list or users via beeline


    return True

def die(code, msg):
    global ignore_errors
    if ignore_errors == True:
        logging.warn(msg)
        logging.warn("carrying on..." )
        return
    logging.error(msg)
    exit(1)

if __name__=="__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout,level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(message)s')
    main()
