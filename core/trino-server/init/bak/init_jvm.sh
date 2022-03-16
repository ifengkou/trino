#!/bin/bash

jvm_xmx='8G'

if [ $# -eq 1 ];then
  jvm_xmx=$1
fi

init_dir=`dirname $0`

jvm_file=${init_dir}/../etc/jvm.config

echo "set jvm.xmx=$jvm_xmx in $jvm_file"

sed -i "s|{{jvm.xmx}}|$jvm_xmx|g" $jvm_file
