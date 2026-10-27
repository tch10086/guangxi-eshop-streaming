#!/bin/bash
# ============================================================
# 一键停止全部组件—— 顺序与启动相反
# 停生产者/流任务 → Hive Metastore → YARN → HDFS → Kafka → ZK
# ============================================================
source /etc/profile.d/bigdata.sh
set -u

log() { echo -e "\033[1;33m>>> $*\033[0m"; }

log "停止 Spark 流任务（如有）"
pkill -f spark_streaming.py 2>/dev/null || true

log "停止 Hive Metastore"
pkill -f "hive.*metastore" 2>/dev/null || true
pkill -f RunJar 2>/dev/null || true

log "停止 YARN / HDFS"
$HADOOP_HOME/sbin/stop-yarn.sh 2>/dev/null || true
$HADOOP_HOME/sbin/stop-dfs.sh 2>/dev/null || true

log "停止 Kafka"
$KAFKA_HOME/bin/kafka-server-stop.sh 2>/dev/null || true
sleep 6

log "停止 Zookeeper"
$KAFKA_HOME/bin/zookeeper-server-stop.sh 2>/dev/null || true
sleep 3

echo
echo "剩余 Java 进程："
jps || true
echo "停止完成。MySQL 仍在运行（systemctl stop mysqld 可关）"
