#!/usr/bin/env bash

export tablename=$1

export purge_cmd=""
if [ "$3" == "true" ]; then
	export purge_cmd="purge"
fi

case "$2" in

prepare)
  /opt/hive/hcatalog/bin/hcat -e "show tables"
  /opt/hive/hcatalog/bin/hcat -e "drop table IF EXISTS ${tablename} ${purge_cmd}"
  /opt/hive/hcatalog/bin/hcat -e "create table ${tablename} ( id INT, name string ) stored as rcfile ;"
;;

cleanup)
  /opt/hive/hcatalog/bin/hcat -e "drop table IF EXISTS ${tablename} ${purge_cmd}"
;;

esac
