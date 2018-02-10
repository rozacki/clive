#!/usr/bin/env bash

target_database=${1:-uc_clive}
sql_script=${2:-rbac.sql}

echo "target database is" $target_database
echo "sql script is" $sql_script

beeline -u "jdbc:hive2://vs-dip-srv01.server.uc:10000/$target_database;principal=hive/vs-dip-srv01.server.uc@UCDIP.LOCAL"D \
        -f $sql_script