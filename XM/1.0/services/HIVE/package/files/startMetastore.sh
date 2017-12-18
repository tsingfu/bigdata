#!/usr/bin/env bash

HIVE_BIN=${HIVE_BIN:-"hive"}

HIVE_CONF_DIR=$4 $HIVE_BIN --service metastore -hiveconf hive.log.file=hivemetastore.log -hiveconf hive.log.dir=$5 > $1 2> $2 &
echo $!|cat>$3
