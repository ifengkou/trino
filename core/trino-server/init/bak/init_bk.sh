#!/bin/bash

echo '初始化config.properties'
is_c=''
while ! [[ $is_c = 'true' || $is_c = 'false' ]];do
    read -p "是否为协调者:(true|false)" is_c
    printf "是否为协调者:$is_c\n"
done

is_cluster=''
while ! [[ "$is_cluster" = 'true' || "$is_cluster" = 'false' ]];do
    read -p "是否为集群版:(true|false)" is_cluster
    printf "是否为集群版=$is_cluster\n"
done

server_num=1

if [ "$is_cluster"='true' ];then
    read -p "集群机器数:(建议>=3)" server_num
    printf "集群机器数=$server_num\n"
fi

server_ip='p'
while [ $server_port -lt 8000  ];do
    read -p "本机ip:(>8000)" server_port
    printf "服务端口=$server_port\n"
done

server_port=0
while [ $server_port -lt 8000  ];do
    read -p "服务端口:(>8000)" server_port
    printf "服务端口=$server_port\n"
done


#max_mem_total=0
#while [ $max_mem_total -lt 8  ];do
#    read -p "整个集群最大可使用内存G:(>=8)" max_mem_total
#    printf "整个集群最大可使用内存G=$max_mem_total \n"
#done
#max_mem_total=$max_mem_total'G'

max_mem_node=0
while [ $max_mem_node -lt 1  ];do
    read -p "单节点单次查询最大可使用内存G:(>=1)" max_mem_node
    printf "单节点单次查询最大可使用内存G=$max_mem_node\n"
done
max_mem_node_total=$[2*$max_mem_node]
max_jvm=$[4*$max_mem_node]
max_mem_total=$[1*$server_num*$max_mem_node_total]
max_mem_node=$max_mem_node'G'

max_mem_node_total=$max_mem_node_total'G'
printf "单节点最大可使用内存=$max_mem_node_total\n"

max_jvm=$max_jvm'G'
printf "单节点jvm -Xmx=$max_jvm\n"

max_mem_total=$max_mem_total'G'
printf "整个集群最大可使用内存G=$max_mem_total \n"

