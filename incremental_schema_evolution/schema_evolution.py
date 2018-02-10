#!/usr/bin/env python

'''
    ~~~~~~~~~~~~~~~~~ This script is POC ~~~~~~~~~~~~~~

    The script have to be invoked from the main clive folder.

    Description:
    This script compares two technical mappings old with a new one and generates new technical mapping. The comparision is based on target table, column and column type.
    The script updates the old mapping making it backward compatible:
     - If table does not exist in the old mapping it's added with all columns
     - if column does not exist then it's added to the end of the old mapping
     - if column exists then its type is checked
        - if types don not match then error is raised

    Initiating:
    When generating mapping for the first time --init flag has to be passed. A new BASE mapping will be generated which
    effectively the copy of the RELEASE mapping
    $ ./schema_evolution.py --i True

    Schema evolution:
    A new DELTA mapping will be generated with suffix -delta so that  base and rlease mappings are intact.
    $ ./schema_evolution.py

    If a new column is found script will generate INFO for example
    2018-01-15 11:42:09,185 INFO: schema_evolution appended column 'DebtInterest.new_column' of type 'string'

    if a new table is found script will generate INFO for example
    2018-01-15 11:44:11,301 INFO: schema_evolution added new table 'new_table', 1 columns to old mappin

'''

import sys
sys.path.append("/Users/chrisrozacki/DIP/uc-schema-analyzer")
sys.path.append("/home/chrisrozacki/DP-4325/uc-schema-analyzer")
import toolkit

import logging
import argparse
import config

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--init", action = "store", required = False, help="init incremental load, if this option is enabled the source "
                                                                                 "data location should point to the full load. A new base mapping will be generated", default=False)
    parser.add_argument("-nm", "--delta_mapping", action = "store", required = False, help="new mapping file from the release folder", default=config.RELEASE_MAPPING)
    parser.add_argument("-om", "--base_mapping", action = "store", required = False, help="base or 'full' mapping file", default=config.BASE_MAPPING)
    parser.add_argument("-o", "--merged_delta_mapping", action = "store", required = False, help="If --init option is not enabled, outputs delta mapping file", default=config.DELTA_MAPPING)
    parser.add_argument("-ts", "--table_suffix", action = "store", required = False, help="suffix added to the table name in delta mapping file to distinguish from the production table", default=config.DELTA_TABLE_SUFFIX)
    args = parser.parse_args()

    if args.init:
        logging.info("init incremental load on")
        args.table_suffix = config.BULK_TABLE_SUFFIX

    generate_backward_compatible_mapping(args)

def generate_backward_compatible_mapping(args):

    # if old mapping does not exist then create it
    base_mapping = toolkit.read_mapping(args.base_mapping, args.init)

    delta_mapping = toolkit.read_mapping(args.delta_mapping)

    # get list of old target tables and convert mapping to dictionary
    base_mapping_dict = {}
    for target_table in toolkit.get_unique_by_column_id(base_mapping, toolkit.TARGET_TABLE):
        base_mapping_dict[target_table] = toolkit.get_target_table_rows(base_mapping, target_table)

    # get list of new target tables and convert mapping to dictionary
    delta_mapping_dict = {}
    for target_table in toolkit.get_unique_by_column_id(delta_mapping, toolkit.TARGET_TABLE):
        delta_mapping_dict[target_table] = toolkit.get_target_table_rows(delta_mapping, target_table)

    # check if new table is in the old mapping, if not add it and all its columns
    logging.info("checking if new tables are in the old mapping")
    for target_table in delta_mapping_dict:
        if not target_table in base_mapping_dict:
            base_mapping_dict[target_table] =  delta_mapping_dict[target_table][:]
            logging.info("added new table '{0}', {1} columns to old mapping".format(target_table, len(delta_mapping_dict[target_table])))

    # for each table check if all columns are in old and has the same type
    logging.info("checking column and column types")
    for target_table in delta_mapping_dict:
        old_columns  =  base_mapping_dict[target_table]
        new_columns = delta_mapping_dict[target_table]
        for new_column in new_columns:
            logging.debug("checking target column '{0}.{1}' type {2}".format(target_table, new_column[toolkit.TARGET_COLUMN],  new_column[toolkit.TARGET_TYPE]))
            found = False
            for old_column in old_columns:
                if new_column[toolkit.TARGET_COLUMN] == old_column[toolkit.TARGET_COLUMN]:
                    found = True
                    if new_column[toolkit.TARGET_TYPE] != old_column[toolkit.TARGET_TYPE]:
                        logging.error("column '{0}' has type '{1}', expected type '{2}'".format(new_column[toolkit.TARGET_COLUMN]
                                                                       , new_column[toolkit.TARGET_TYPE], old_column[toolkit.TARGET_TYPE]))
                    else:
                        logging.debug("'{0}' column match with old".format(new_column[toolkit.TARGET_COLUMN]))
                    break
            if not found:
                # append column
                old_columns.append(new_column)
                logging.info("appended column '{0}.{1}' of type '{2}'".format(target_table, new_column[toolkit.TARGET_COLUMN],new_column[toolkit.TARGET_TYPE]))

    logging.info("base mapping persisted to '{0}'".format(args.base_mapping) )
    toolkit.save_mapping_as_dictionary(args.base_mapping, base_mapping_dict)

    # append suffix to target table name
    for target_table in base_mapping_dict:
        for old_column in base_mapping_dict[target_table]:
            old_column[toolkit.TARGET_TABLE] =  old_column[toolkit.TARGET_TABLE]  + args.table_suffix
            pass

    if not args.init:
        logging.info("delta mapping persisted to '{0}'".format(args.merged_delta_mapping) )
        if not args.init:
            toolkit.save_mapping_as_dictionary(args.merged_delta_mapping, base_mapping_dict)

if __name__=="__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout,level=logging.INFO, format='%(asctime)s %(levelname)s: %(module)s %(message)s')
    main()
