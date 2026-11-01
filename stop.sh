#!/bin/bash
# ============================================================================
#  一键关闭全部 —— 只跑这一个脚本
#    用法：  bash stop.sh
#  顺序：生产者 → 流任务 → Flask → 大屏 → Hive Metastore → YARN → HDFS
#        → Kafka → Zookeeper → MySQL
#  重复执行安全：已停的会跳过
# ============================================================================
set -u
source /etc/profile.d/bigdata.sh 2>/dev/null

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE" || exit 1

log() { echo -e "\033[1;33m>>> $*\033[0m"; }
ok()  { echo -e "    \033[1;32m✔ $*\033[0m"; }

echo "=================================================================="
echo "  一键关闭   $(date '+%F %T')"
echo "=================================================================="

log "1/10 订单回放器"
pkill -f kafka_producer.py 2>/dev/null && ok "已停止" || ok "未运行"

log "2/10 Spark 流任务"
pkill -f spark_streaming.py 2>/dev/null && ok "已停止" || ok "未运行"
sleep 3

log "3/10 Flask 接口（5000）"
pkill -f flask_server.py 2>/dev/null && ok "已停止" || ok "未运行"

log "4/10 大屏静态服务（8000）"
pkill -f "http.server 8000" 2>/dev/null && ok "已停止" || ok "未运行"

log "5/10 Hive Metastore"
pkill -f "hive.*metastore" 2>/dev/null; pkill -f RunJar 2>/dev/null
ok "已停止"
sleep 2

log "6/10 YARN"
$HADOOP_HOME/sbin/stop-yarn.sh >/dev/null 2>&1; ok "已停止"

log "7/10 HDFS"
$HADOOP_HOME/sbin/stop-dfs.sh >/dev/null 2>&1; ok "已停止"
sleep 3

log "8/10 Kafka"
$KAFKA_HOME/bin/kafka-server-stop.sh >/dev/null 2>&1
for i in $(seq 1 10); do jps | grep -q " Kafka$" || break; sleep 2; done
pkill -f kafka.Kafka 2>/dev/null
ok "已停止"

log "9/10 Zookeeper"
$KAFKA_HOME/bin/zookeeper-server-stop.sh >/dev/null 2>&1
for i in $(seq 1 8); do jps | grep -q QuorumPeerMain || break; sleep 2; done
pkill -f QuorumPeerMain 2>/dev/null
ok "已停止"

log "10/10 MySQL"
systemctl stop mysqld 2>/dev/null
ok "已停止，当前状态：$(systemctl is-active mysqld 2>/dev/null)"

echo
echo "------------------------------------------------------------------"
echo " 剩余 Java 进程："
jps 2>/dev/null | grep -v Jps || echo "   无"
echo "------------------------------------------------------------------"
echo " 全部已关闭。要关机请再执行： poweroff"
echo "=================================================================="
