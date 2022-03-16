#!/bin/bash

node_id='001'
node_data_dir='/data/tingyun/trino/data'

if [ $# -lt 1 ];then
  echo "ERROR: please enter [node_id] like 001/002..."
  exit -1
fi

if [ $# -eq 1 ];then
  node_id=$1
fi

if [ $# -eq 2 ];then
  node_id=$1
  node_data_dir=$2
fi

init_dir=`dirname $0`

_file=${init_dir}/../etc/node.properties

echo "set node.id=$node_id in $_file"
sed -i "s|{{node.id}}|$node_id|g" $_file

echo "set node.data-dir=$node_data_dir in $_file"
sed -i "s|{{node.data-dir}}|$node_data_dir|g" $_file
