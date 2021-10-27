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
colorEcho $BLUE "===初始化coordinator==="

sh $init_dir/bash.sh
if [ $? -ne 0 ]; then
    colorEcho $RED "选择集群配置脚本执行失败"
    exit -1
else
    colorEcho $GREEN "选择集群配置脚本执行成功"
fi

# 修改node_id
node_id="001"
node_file=${init_dir}/../etc/node.properties

echo "" > $node_file
echo "node.environment=production" > $node_file
echo "node.id=ffffffff-ffff-ffff-ffff-ffffffffffff-$node_id" >> $node_file
echo "node.data-dir=/data/tingyun/trino/data" >> $node_file

#echo "set node.id=$node_id in $node_file"
#sed -i "s|{{node.id}}|$node_id|g" $node_file

if [ $? -ne 0 ]; then
    colorEcho $RED "(set node.id=001) failed"
else
    colorEcho $GREEN "(set node.id=001) succeed"
fi

# rm config_worker.properties
worker_file=${init_dir}/../etc/config_worker.properties
rm -f $worker_file

colorEcho $GREEN "===初始化coordinator succeed==="
