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

colorEcho $BLUE "===初始化当前worker==="

sh $init_dir/bash.sh
if [ $? -ne 0 ]; then
    colorEcho $RED "选择集群配置脚本执行失败"
    exit -1
else
    colorEcho $GREEN "选择集群配置脚本执行成功"
fi

# 当前第几台机器
node_num=1
while [ $node_num -lt 2  ];do
    read -p "当前是第几台机器[2/3/4/5/6]:" node_num
    colorEcho $YELLOW "当前是第( $node_num )台机器"
done

# 修改node_id
node_id="00$node_num"
node_file=${init_dir}/../etc/node.properties

echo "" > $node_file
echo "node.environment=production" > $node_file
echo "node.id=ffffffff-ffff-ffff-ffff-ffffffffffff-$node_id" >> $node_file
echo "node.data-dir=/data/tingyun/trino/data" >> $node_file

#echo "set node.id=$node_id in $node_file"
#sed "s|{{node.id}}|$node_id|g" $node_file

if [ $? -ne 0 ]; then
    colorEcho $RED "(sed node.id=$node_id) failed"
else
    colorEcho $GREEN "(sed node.id=$node_id) succeed"
fi

# 将 config_worker.properties 覆盖config.properties
worker_file=${init_dir}/../etc/config_worker.properties
config_file=${init_dir}/../etc/config.properties
mv $worker_file $config_file

if [ $? -ne 0 ]; then
    colorEcho $RED "(mv config_worker.properties config.properties) failed"
else
    colorEcho $GREEN "(mv config_worker.properties config.properties) succeed"
fi

colorEcho $GREEN "===初始化当前worker succeed==="
