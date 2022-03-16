#!/bin/bash

log_level='INFO'

if [ $# -eq 1 ];then
  log_level=$1
fi

init_dir=`dirname $0`

log_file=${init_dir}/../etc/log.properties

echo "set log_level=$log_level in $log_file"

sed -i "s|{{log.level}}|$log_level|g" $log_file
