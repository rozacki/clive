#!/usr/bin/env python

'''
author: Chris Rozacki
To generate no pii views from mapping:
1. concat all mapping files together into mappnig.csv
2. $ ./toolkit.py --generate-no-pii-view -p release/mapping.csv
'''

import logging
import sys
import os
import subprocess
import argparse
import csv
import glob
import errno

# column indexes in mapping
SOURCE_DATABASE = 0
SOURCE_COLLECTION = 1
SOURCE_JSONPATH = 2
SOURCE_TYPE = 3
TARGET_TABLE = 4
TARGET_COLUMN = 5
TARGET_TYPE = 6
FUNCTION = 7
META = 8

MAPPING_HEADER = "sourcesourceDB,sourceCollection,sourceFieldLocation,sourceDataType,destinationTable,destinationField,destinationDataType,function,meta"

JIVE_JAR = "./jive.jar"

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--path", action = "store", required = True, help="for splitting mappings it is the csv file, "
                                                                                "for generating sql it is the folder with mapping files, "
                                                                                "for generating no-pii view it is source technical mapping file"
                                                                                "for combine-mapping-files it is folder path")
    parser.add_argument("-l", "--location", action = "store", required = False, help="hdfs data location", default="/etl/uc/mongo/${hiveconf:BATCH_DATE}")
    parser.add_argument("-o", "--output-file", action = "store", required = False, help="target sql file or combined mapping file")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--split-mappings', action="store_true", help="split technical mapping into multiple mapping files in current folder")
    group.add_argument('--generate-sql', action="store_true", help="generate aggregated sql from multiple mapping files")
    group.add_argument('--generate-no-pii-view', action="store_true", help="generate no-pii view, a view where all sensitive columns are excluded from")
    group.add_argument('--combine-mapping-files', action="store_true", help="combines mapping file from many mappings in current folder")
    group.add_argument('--generate_dataquality',action="store_true", help="generate Clive data quality HQL statements to test the clive load")
    group.add_argument('--clean_clive_tables',action="store_true", help="Clean previous release clive tables")
    group.add_argument('--generate_rbac_sql',action="store_true", help="Clean previous release clive tables")
    args = parser.parse_args()

    if args.split_mappings:
        split_mappings_file(args.path)
        exit(0)

    if args.generate_sql:
        generate_sql_from_folder(args.path, args.output_file, args.location)
        exit(0)

    if args.generate_no_pii_view:
        generate_no_pii_view(args.path)
        exit(0)

    if args.combine_mapping_files:
        combine_mapping_files(args.path, args.output_file)
        exit(0)

    if args.generate_dataquality:
        generate_dataquality(args.path)
        exit(0)

    if args.clean_clive_tables:
        clean_clive_tables(args.path)
        exit(0)

    if args.generate_rbac_sql:
        generate_rbac_sql(args.path)
        exit(0)


def silentremove(filename):
    ''' swollows exception when file does not exist'''
    try:
        os.remove(filename)
    except OSError as e: # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occurred

'''runs mongo scripts and capture output to the file'''
def run_mongo_script(mongo_script, output_file, eval = ""):
    cmd = [
        'mongo', '--quiet', "--eval", eval, mongo_script
    ]
    logging.debug(cmd)
    try:

        rc = subprocess.Popen(cmd, stdout = subprocess.PIPE)
        content, err = rc.communicate()
        if err:
            logging.error("error while running mongo script " + err)
            exit(1)

        # store in file
        with open(output_file,"w") as file:
            file.write(content)

    except Exception as ex:
        logging.error("exception while running mongo script %s" % (str(ex)))
        exit(1)

'''combines multiple mapping file into single in a folder'''
def combine_mapping_files(folder, mapping_file_name):
    mappings = [each for each in os.listdir(folder) if each.endswith('.csv')]
    # we will store all script in this file
    mapping_file = open(mapping_file_name,"w")

    first = True
    # for all found files generate SQL transform scripts and append to sql_file_name
    for mapping in mappings:
        logging.info("combine mapping file %s" % (os.path.join(folder,mapping)))

        with open(os.path.join(folder,mapping)) as file:
            lines = file.readlines()

            if not first:
                # remove header line
                del lines[0]
            first = False

            for line in lines:
                mapping_file.write(line)

def split_mappings_file(file_name):
    ''' Looks for pattern 'target_table=...' in csv file (produced by javascript function print_mapping_splittable)
        and splits mappings to  separate files.
    '''

    try:
        # read all lines to array
        #lines = []
        with open(file_name) as file:
            lines = file.readlines()

        target_file = None
        for line in lines:

            ''' check if this line is new mapping delimiter
                if it is then try to close previous file
                and use it to open the new file
            '''
            pos = line.find("target_table=")
            if  pos != -1:
                targe_table_name = line[len("target_table="):len(line)-1]
                logging.debug("found target table " + targe_table_name)

                # if open the close current file
                if target_file:
                    target_file.close()

                # open new file
                target_file = open(targe_table_name +".csv","w")

                continue

            #otherwise write line to the file
            target_file.write(line)


    except Exception as ex:
        print("Error reading mappings file " + file_name + ": " + str(ex))
        exit(1)

def generate_sql_from_folder(folder, sql_file_name, location):
    '''
    reads mapping files from provided path, generate sql and stores into single script
    '''

    mappings = [each for each in os.listdir(folder) if each.endswith('.csv')]

    # we will store all script in this file
    sql_file = open(sql_file_name,"w")

    ''' for all found files generate SQL transform scripts and append to sql_file_name
    '''
    for mapping in mappings:
        logging.info("generate sql for mapping file %s" % (os.path.join(folder,mapping)))

        # call jive to generate SQL
        cmd = [
            'java', '-cp', JIVE_JAR, 'uk.gov.dwp.uc.dip.schemagenerator.SchemaGenerator'
            , '-tm', os.path.join(folder,mapping), '-l', location, "-orc"
        ]

        logging.debug("jive command line parameters " + str(cmd))
        try:
            rc = subprocess.Popen(cmd, stdout = subprocess.PIPE)
            content, err = rc.communicate()
            if err:
                logging.error("error while generating sql transformation scripts %s" % (sql_file_name))
                exit(1)

            # append to sql_file
            sql_file.write(content)

        except Exception as ex:
            logging.error("error while generating sql transformation scripts %s" % (os.path.join(folder,mapping)))
            logging.error( "exception details " + str(ex))
            exit(1)

def generate_sql(mapping, sql_file_name, location, where = None, storeTableAs = "orc"):
    logging.info("generate sql for mapping file %s" % (mapping))

    sql_file = open(sql_file_name,"w")

    # call jive to generate SQL
    cmd = [
        'java', '-cp', JIVE_JAR, 'uk.gov.dwp.uc.dip.schemagenerator.SchemaGenerator'
        , '-tm', mapping, '-l', location, "-" + storeTableAs
    ]

    if where:
        cmd.append( "-where" )
        cmd.append( where )

    logging.debug("jive command line parameters " + str(cmd))
    try:
        rc = subprocess.Popen(cmd, stdout = subprocess.PIPE)
        content, err = rc.communicate()
        if err:
            logging.error("error while generating sql transformation scripts %s" % (sql_file_name))
            exit(1)

        # append to sql_file
        sql_file.write(content)

    except Exception as ex:
        logging.error("error while generating sql transformation scripts %s" % (os.path.join(folder,mapping)))
        logging.error( "exception details " + str(ex))
        exit(1)

    return True

''' returns list of ... ?'''
def read_mapping(file_name, create_new = False):
    try:
        # if file does remove and create empty one
        if create_new:
            silentremove(file_name)
            with open(file_name, "w") as file:
                file.write(MAPPING_HEADER)
        # added 'rU' as new-line character seen in unquoted field - do you need to open the file in universal-newline mode?
        with open(file_name, 'rU') as file:
            # do delimiter here as we want list of lists
            reader = csv.reader(file, delimiter=',')
            mapping = list()
            for row in list(reader):
                mapping.append(row)
            return mapping
        return True
    except Exception as ex:
        logging.error("Error opening file " + file_name + ": " + str(ex))
        exit(1)

''' saves technical mapping when provided as dictionary of mappings'''
def save_mapping_as_dictionary(file_name, dict):

    try:
        with open(file_name,"w") as file:
            file.write(MAPPING_HEADER + "\n")

            for target_table in dict:
                for row in dict[target_table]:
                    for index, column in enumerate(row):
                        file.write(column)
                        #print column
                        if index < 8:
                            file.write(",")
                    file.write("\n")

        return True

    except Exception as ex:
        logging.error("Error opening file " + file_name + ": " + str(ex))
        return False


''' returns all target tables '''
def get_unique_by_column_id(mapping, column_name):
    all_target_tables = list()
    for item in mapping:
        all_target_tables.append(item[column_name])
    # remove header
    del all_target_tables[0]
    return list(set(all_target_tables))

''' returns all columns for provided target table'''
def get_target_table_rows(mapping, table):
    rows = list()
    for item in mapping:
        if item[TARGET_TABLE] == table:
            rows.append(item)
    return rows

''' generates no-pii views based on technical mapping meta column'''
def generate_no_pii_view(mapping_file_name):
    mappings = read_mapping(mapping_file_name)
    target_tables = get_unique_by_column_id(mappings, TARGET_TABLE)

    logging.debug("generating no-pii views from {0},  number of rows in technical mapping {1}".format(mapping_file_name, len(mappings)))

    for target_table in target_tables:
        logging.debug("target table '{0}'".format(target_table))
        columns = []
        mapping = get_target_table_rows(mappings, target_table)
        logging.debug("found {0} rows".format(len(mapping)))
        for row in mapping:
            # if last column is empty that item in the list does not exist
            if len(row)>=META:
                logging.debug(row[META])
                pii_idx = row[META].find("PII")
                if pii_idx != -1:
                    # only if PII:NO we include it
                    if row[META][pii_idx:pii_idx + 6] == "PII:NO":
                        columns.append(row[TARGET_COLUMN])
                        #else:
                        #logging.debug("PII found, column not included into the view")
                        #else:
                        #logging.debug("PII not found, column not included into the view")
        sql = get_create_view_sql(target_table,columns,"no_pii")
        if sql:
            print sql

'''produces create view statement based on source target name and list of columns'''
def get_create_view_sql(table, columns, suffix):
    if len(columns) > 0:
        columns_list = ""
        for index, column in enumerate(columns):
            if index > 0:
                columns_list += ", "
            columns_list += column
        sql = "DROP VIEW IF EXISTS {0}_{1};\nCREATE VIEW {0}_{1} as SELECT {2} FROM {3};\n".format(table, suffix, columns_list, table)

        return sql;
    return

def generate_dataquality(mapping_file_name):
    mappings = read_mapping(mapping_file_name)
    target_tables = get_unique_by_column_id(mappings, TARGET_TABLE)
    for targettable in  target_tables:
        sql = get_dataquality_sql(targettable)
        print sql

'''produces select statement based on target table name to enable data quality check on Clive'''
def get_dataquality_sql(table):
    if len(table) > 0:
        sql = "insert into clive_stats SELECT nvl(max('{0}'), '{0}'),  nvl(COUNT(*),0),COUNT(*)-count(id),min(created_ts),max(created_ts),min(removed_ts) ,max(removed_ts),min(last_modified_ts) ,max(last_modified_ts) FROM uc_clive.{0};".format(table)

        return sql;
    return

'''helper to remove all previous release tables'''
def clean_clive_tables(mapping_file_name):
    mappings = read_mapping(mapping_file_name)
    target_tables = get_unique_by_column_id(mappings, TARGET_TABLE)
    for targettable in  target_tables:
        sql = "drop table IF EXISTS {0};".format(targettable)
        print sql

'''generate rbac statement'''
def generate_rbac_sql(mapping_file_name):
    mappings = read_mapping(mapping_file_name)
    target_tables = get_unique_by_column_id(mappings, TARGET_TABLE)
    for targettable in  target_tables:
        sql = "GRANT SELECT ON  {0}  TO ROLE CLIVE;".format(targettable)
        print sql


'''helper to remove all files from a folder'''
def remove_all_files(folder):
    files = glob.glob(folder + '/*')
    for f in files:
        os.remove(f)

'''helper to prepend some content to a file'''
def prepend_to_file(file_name, content):
    lines = None
    with open(file_name) as file:
        lines = file.readlines()

    with open(file_name,"w") as file:
        file.write(content)
        for line in lines:
            file.write(line)

if __name__=="__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout,level=logging.INFO, format='%(asctime)s %(levelname)s %(module)s %(message)s')
    main()