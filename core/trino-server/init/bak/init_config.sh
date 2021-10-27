#!/bin/bash
:<<!
coordinator={{is_coordinator}}
node-scheduler.include-coordinator={{is_include_coord}}
http-server.http.port={{server_port}}
query.max-memory={{max_mem_total}}
query.max-memory-per-node={{max_mem_node}}
query.max-total-memory-per-node={{max_mem_node_total}}
discovery-server.enabled=true
discovery.uri={{discovery_uri}}

trino.extension.jdbc.enable=true
trino.extension.jdbc.driver={{ext_jdbc_driver}}
trino.extension.jdbc.url={{ext_jdbc_url}}
trino.extension.jdbc.username={{ext_jdbc_username}}
trino.extension.jdbc.password={{ext_jdbc_password}}
trino.extension.jdbc.max-pool-size={{ext_jdbc_pool_size}}

dynamic-catalog.enable={{ext_dycata_enable}}
dynamic-catalog.table-name={{ext_dycata_table}}
!

is_coordinator = 'false'
is_cluster='true'
server_port='8080'
max_mem_total='12G'
max_mem_node='2G'
max_mem_node_total='4G'

discovery_uri="http://$local_ip:$server_port"

ext_jdbc_driver='com.mysql.jdbc.Driver'
ext_jdbc_url='jdbc:mysql://mysql-1:33061'
ext_jdbc_username='test'
ext_jdbc_password='test'
ext_jdbc_pool_size='3'

ext_dycata_enable='true'
ext_dycata_table='db_test.catalog'

if [ $# -ne 14 ];then
  echo "ERROR: please enter:   "
  echo "       [is_coordinator] [is_cluster] [local_ip] [server_port]  "
  echo "       [max_mem_total] [max_mem_node] [max_mem_node_total]  "
  echo "       [ext_jdbc_driver] [ext_jdbc_url]  "
  echo "       [ext_jdbc_username] [ext_jdbc_password] [ext_jdbc_pool_size]  "
  echo "       [ext_dycata_enable] [ext_dycata_table]  "
  echo "  total 14 params  "
  exit -1
fi

is_coordinator = $1
is_cluster=$2
local_ip=$3
server_port=$4
max_mem_total=$5
max_mem_node=$6
max_mem_node_total=$7

discovery_uri="http://$local_ip:$server_port"

ext_jdbc_driver=$8
ext_jdbc_url=$9
ext_jdbc_username=${10}
ext_jdbc_password=${11}
ext_jdbc_pool_size=${12}

ext_dycata_enable=${13}
ext_dycata_table=${14}


init_dir=`dirname $0`

_file=${init_dir}/../etc/config.properties
echo "config.file=$_file"
echo "set is_coordinator=$is_coordinator"
sed -i "s|{{is_coordinator}}|$is_coordinator|g" $_file

is_include_coord='false'
if [$is_coordinator == 'true' ] && [$is_cluster == 'true'];then
    is_include_coord='true'
fi
echo "set is_include_coord=$is_include_coord"
sed -i "s|{{is_include_coord}}|$is_include_coord|g" $_file

echo "set server_port=$server_port"
sed -i "s|{{server_port}}|$server_port|g" $_file

echo "set max_mem_total=$max_mem_total"
sed -i "s|{{max_mem_total}}|$max_mem_total|g" $_file

echo "set max_mem_node=$max_mem_node"
sed -i "s|{{max_mem_node}}|$max_mem_node|g" $_file

echo "set max_mem_total=$max_mem_total"
sed -i "s|{{max_mem_total}}|$max_mem_total|g" $_file

echo "set max_mem_node_total=$max_mem_node_total"
sed -i "s|{{max_mem_node_total}}|$max_mem_node_total|g" $_file

echo "set discovery_uri=$discovery_uri"
sed -i "s|{{discovery_uri}}|$discovery_uri|g" $_file

echo "set ext_jdbc_driver=$ext_jdbc_driver"
sed -i "s|{{ext_jdbc_driver}}|$ext_jdbc_driver|g" $_file

echo "set ext_jdbc_url=$ext_jdbc_url"
sed -i "s|{{ext_jdbc_url}}|$ext_jdbc_url|g" $_file

echo "set ext_jdbc_username=$ext_jdbc_username"
sed -i "s|{{ext_jdbc_username}}|$ext_jdbc_username|g" $_file

echo "set ext_jdbc_password=$ext_jdbc_password"
sed -i "s|{{ext_jdbc_password}}|$ext_jdbc_password|g" $_file

echo "set ext_jdbc_pool_size=$ext_jdbc_pool_size"
sed -i "s|{{ext_jdbc_pool_size}}|$ext_jdbc_pool_size|g" $_file

echo "set ext_dycata_enable=$ext_dycata_enable"
sed -i "s|{{ext_dycata_enable}}|$ext_dycata_enable|g" $_file

echo "set ext_dycata_table=$ext_dycata_table"
sed -i "s|{{ext_dycata_table}}|$ext_dycata_table|g" $_file
