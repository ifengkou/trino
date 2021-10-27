#!/bin/bash
# coordinator 配置初始化脚本
# Author: shenlg@tingyun.com

init_dir=`dirname $0`

RED="\033[31m"      # Error message
GREEN="\033[32m"    # Success message
YELLOW="\033[33m"   # Warning message
BLUE="\033[36m"     # Info message
PLAIN='\033[0m'

colorEcho() {
    echo -e "${1}${@:2}${PLAIN}"
}


cluster_echo(){
    colorEcho $BLUE '选择当前部署的集群配置：'
    echo "  1) 1节点8G内存"
    echo "  2) 3节点8G内存"
    echo "  3) 3节点16G内存"
    echo "  4) 3节点32G内存"
    echo "  5) 3节点64G内存"
    echo "  6) 3节点128G内存"
    echo "  7) 6节点32G内存"
}


server_package=0
while ! [[ $server_package -ge 1 && $server_package -le 7 ]];do
    cluster_echo
    read -p "请选择集群配置[1-7]:" server_package
done
colorEcho $GREEN "选择的集群配置为：( $server_package )"

config_dir=${init_dir}/../etc/
if [ $server_package -eq 1 ];then
    config_files=${init_dir}/../etc/1_8G/*
elif [ $server_package -eq 2 ]; then
    config_files=${init_dir}/../etc/3_8G/*
elif [ $server_package -eq 3 ]; then
    config_files=${init_dir}/../etc/3_16G/*
elif [ $server_package -eq 4 ]; then
    config_files=${init_dir}/../etc/3_32G/*
elif [ $server_package -eq 5 ]; then
    config_files=${init_dir}/../etc/3_64G/*
elif [ $server_package -eq 6 ]; then
    config_files=${init_dir}/../etc/3_128G/*
elif [ $server_package -eq 7 ]; then
    config_files=${init_dir}/../etc/6_32G/*
fi

cp $config_files $config_dir

if [ $? -ne 0 ]; then
    colorEcho $RED "(cp $config_files $config_dir) failed"
else
    colorEcho $GREEN "(cp $config_files $config_dir) succeed"
fi

# check file
echo_cp_tip(){
    colorEcho $RED "请手动从{trino-base}/etc/{集群配置}目录中拷贝文件到{trino-base}/etc目录"
}
config_file=$config_dir'config.properties'
jvm_file=$config_dir'jvm.config'
if [ ! -f "$config_file" ]; then
    colorEcho $RED "{trino-base}/etc/目录下未找到config.properties"
    echo_cp_tip
    exit -1
fi
if [ ! -f "$jvm_file" ]; then
    colorEcho $RED "{trino-base}/etc/目录下未找到jvm.config"
    echo_cp_tip
    exit -1
fi

if [ $server_package -gt 1 ];then
    worker_file=$config_dir'config_worker.properties'
    if [ ! -f "$worker_file" ]; then
        colorEcho $RED "{trino-base}/etc/目录下未找到config_worker.properties"
        echo_cp_tip
        exit -1
    fi
fi

# 修改mysql jdbc url
db_url=""
read -p "请输入bpi_conf数据库的IP/Hostname和端口[10.128.xx.xx:3306]:" db_url
echo "jdbc.url=$db_url"
sed -i "s|{{mysql_jdbc_url}}|$db_url|g" $config_file
