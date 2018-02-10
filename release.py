#!/usr/bin/env python

'''
author: Chris Rozacki
This script generates mapping (csv) and transformation (sql) scripts based on Json Schema
'''

import toolkit
import os
import logging
import sys
import glob
import argparse

SQL_FILE            =   "release.sql"
CSV_FILE            =   "release.csv"
NO_PII              =   "no_pii.sql"
TABLES              =   "tables"
TEMP_DIR            =   "temp"
WORKING_CSV         =   "working.csv"
DATA_LOCATION       =   "/etl/uc/mongo/${hiveconf:BATCH_DATE}"
RELEASE_FOLDER      =   "release"
VERSION             =   "r84"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mapping-only", action = "store", required = False, help="generate mapping files only")
    parser.add_argument("-d", "--database_name", action = "store", required = False, help="database name where all transform will be done", default="change_me")
    args = parser.parse_args()

    mapping_only = False
    if args.mapping_only:
        mapping_only = True

    generate_model_a(mapping_only)

    generate_model_b(mapping_only)

    generate_model_c(mapping_only)

    generate_model_d(mapping_only)

    # prepend use database name to each sql file
    files = glob.glob(RELEASE_FOLDER + '/*.sql')
    for f in files:
        toolkit.prepend_to_file(f, "use {0};\n".format(args.database_name))

def generate_model_a(mapping_only = False):
    '''
    I
    jive accepts one location per mapping file hence we split mappings into individual files.
    Generate splittable mapping
    $ mongo -quiet DATABASE_NAME.js > DATABASE_NAME.csv

    II
    Split mapping in folder from DATABASE_NAME.csv
    $ cd temp
    $ ../toolkit.py -p ../DATABASE_NAME.csv --split-mappings

    III
    Generate sql from mappings from folder
    $ cd ..
    $ ./toolkit.py --generate-sql -o DATABASE_NAME.sql -p temp

    IV
    Combine one mapping from all mappings in a folder
    $ ./toolkit.py --combine-mapping-files -p temp -o DATABASE_NAME.csv
    '''

    SQL_FILE = "release/" + "model_a_collections.sql"
    CSV_FILE = "release/" + "model_a_collections.csv"

    #
    toolkit.run_mongo_script("generate_model_a.js", CSV_FILE, "var schema_collection_name='schema-{0}'".format(VERSION))
    if mapping_only:
        return
    toolkit.generate_sql(CSV_FILE, SQL_FILE, DATA_LOCATION)

def generate_model_b(mapping_only = False):
    '''
    generate snowflake model from one collection with where statement

    I
    Generate technical mapping from UC schema
    mongo --quiet --eval "var database='agenttodo', method=''" agenttodo.js > agenttodo.csv

    II
    Generate where clauses
    mongo --quiet --eval "var database='agenttodo', method='where'" agenttodo.js > agenttodo-where.csv

    III
    Generate SQL
    java -cp jive.jar uk.gov.dwp.uc.dip.schemagenerator.SchemaGenerator -tm agenttodo.csv -where agenttodo-where.csv -l /etl/uc/mongo/${hiveconf:BATCH_DATE} -orc

    '''

    SQL_FILE = "release/model_b_collections.sql"
    WORKING_CSV = "release/model_b_collections.csv"
    WHERE_CSV = "model_b_collections-where.csv"

    #
    #
    toolkit.run_mongo_script("generate_model_b.js", WORKING_CSV, "var method='', schema_collection_name='schema-{0}'".format(VERSION))
    if mapping_only:
        return
    #generate where csv
    toolkit.run_mongo_script("generate_model_b.js", WHERE_CSV, "var method='where', schema_collection_name='schema-{0}'".format(VERSION))
    #
    toolkit.generate_sql(WORKING_CSV, SQL_FILE, DATA_LOCATION, WHERE_CSV)

def generate_model_c(mapping_only = False):
    '''
    I
    jive accepts one location per mapping file hence we split mappings into individual files.
    Generate splittable mapping
    $ mongo -quiet DATABASE_NAME.js > DATABASE_NAME.csv

    II
    Split mapping in folder from DATABASE_NAME.csv
    $ cd temp
    $ ../toolkit.py -p ../DATABASE_NAME.csv --split-mappings

    III
    Generate sql from mappings from folder
    $ cd ..
    $ ./toolkit.py --generate-sql -o DATABASE_NAME.sql -p temp

    IV
    Combine one mapping from all mappings in a folder
    $ ./toolkit.py --combine-mapping-files -p temp -o DATABASE_NAME.csv
    '''

    SQL_FILE = "release/model_c_collections.sql"
    CSV_FILE = "release/model_c_collections.csv"

    #
    toolkit.run_mongo_script("generate_model_c.js", CSV_FILE, "var schema_collection_name='schema-{0}'".format(VERSION))
    if mapping_only:
        return
    toolkit.generate_sql(CSV_FILE, SQL_FILE, DATA_LOCATION)

def generate_model_d(mapping_only = False):
    '''
    generate snowflake model from one collection with where statement

    I
    Generate technical mapping from UC schema
    mongo --quiet --eval "var database='agenttodo', method=''" agenttodo.js > agenttodo.csv

    II
    Generate where clauses
    mongo --quiet --eval "var database='agenttodo', method='where'" agenttodo.js > agenttodo-where.csv

    III
    Generate SQL
    java -cp jive.jar uk.gov.dwp.uc.dip.schemagenerator.SchemaGenerator -tm agenttodo.csv -where agenttodo-where.csv -l /etl/uc/mongo/${hiveconf:BATCH_DATE} -orc

    '''

    SQL_FILE = "release/model_d_collections.sql"
    WORKING_CSV = "release/model_d_collections.csv"
    WHERE_CSV = "model_d_collections-where.csv"

    #
    #
    toolkit.run_mongo_script("generate_model_d.js", WORKING_CSV, "var method='', print_out=true, schema_collection_name='schema-{0}'".format(VERSION))
    if mapping_only:
        return
    #generate where csv
    toolkit.run_mongo_script("generate_model_d.js", WHERE_CSV, "var method='where', print_out=true, schema_collection_name='schema-{0}'".format(VERSION))
    #
    toolkit.generate_sql(WORKING_CSV, SQL_FILE, DATA_LOCATION, WHERE_CSV)

if __name__=="__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout,level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(message)s')
    main()