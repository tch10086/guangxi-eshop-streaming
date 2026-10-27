#!/bin/bash
# ============================================================
# 一键启动全部组件
# 顺序：MySQL → Zookeeper → Kafka → HDFS → YARN → Hive Metastore
# 注意：必须先 ZK 再 Kafka，先 HDFS 再 Hive，否则会连不上
# ============================================================
source /etc/profile.d/bigdata.sh
set -u

log() { echo -e "\033[1;36m>>> $*\033[0m"; }

log "1/6 启动 MySQL"
systemctl start mysqld 2>/dev/null || systemctl start mariadb 2>/dev/null || echo "  MySQL 启动失败或已运行"

log "2/6 启动 Zookeeper"
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
sleep 4

log "3/6 启动 Kafka"
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
sleep 8

log "4/6 启动 HDFS"
$HADOOP_HOME/sbin/start-dfs.sh

log "5/6 启动 YARN"
$HADOOP_HOME/sbin/start-yarn.sh

log "6/6 启动 Hive Metastore（9083）"
if ! jps | grep -q RunJar; then
  nohup $HIVE_HOME/bin/hive --service metastore >/tmp/metastore.log 2>&1 &
  sleep 8
fi

echo
echo "=========== 状态检查 ==========="
jps
echo "--- Kafka Topic ---"
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server master:9092 2>/dev/null
echo "--- HDFS ---"
$HADOOP_HOME/bin/hdfs dfs -ls / 2>/dev/null | head -3
echo "================================"
echo "全部启动完成。Web：HDFS http://<IP>:9870   YARN http://<IP>:8088"
