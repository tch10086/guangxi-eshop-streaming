#!/bin/bash
# ============================================================================
#  一键启动全部 —— 只跑这一个脚本
#    用法：  bash start.sh              # 组件 + 接口 + 大屏 + 流任务 + 订单回放
#            bash start.sh --no-producer # 不自动回放订单（数字不涨，只看现状）
#            bash start.sh --rate 20     # 回放速度（条/秒，默认 8）
#  顺序：MySQL → ZK → Kafka → HDFS → YARN → Hive Metastore → Flask → 大屏 → 流任务 → 生产者
#  幂等：已在跑的组件跳过；Kafka 遇到 ZK 残留节点会自动清理后重试
#  日志：/tmp/start.log  /tmp/flask.log  /tmp/httpd.log  /tmp/stream.log  /tmp/producer.log
# ============================================================================
set -u
source /etc/profile.d/bigdata.sh 2>/dev/null

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE" || exit 1

WITH_PRODUCER=1
RATE=8
while [ $# -gt 0 ]; do
  case "$1" in
    --no-producer) WITH_PRODUCER=0; shift ;;
    --rate) RATE="${2:-8}"; shift 2 ;;
    *) echo "未知参数：$1"; exit 1 ;;
  esac
done

PY=/opt/module/miniconda3/bin/python
IP=$(hostname -I | awk '{print $1}')
log() { echo -e "\033[1;36m>>> $*\033[0m"; }
ok()  { echo -e "    \033[1;32m✔ $*\033[0m"; }
bad() { echo -e "    \033[1;31m✘ $*\033[0m"; }
port_up() { ss -lnt 2>/dev/null | grep -q ":$1 "; }

echo "=================================================================="
echo "  广西特色农产品电商订单 · 流批一体分析系统   一键启动"
echo "  时间：$(date '+%F %T')   主机：$(hostname)   IP：$IP"
echo "=================================================================="

# ---------------------------------------------------------------- 1 MySQL
log "1/9  MySQL"
if systemctl is-active --quiet mysqld; then
  ok "已在运行"
else
  systemctl start mysqld
  for i in $(seq 1 15); do systemctl is-active --quiet mysqld && break; sleep 2; done
  systemctl is-active --quiet mysqld && ok "已启动" || bad "启动失败：journalctl -u mysqld -n 30"
fi

# ---------------------------------------------------------------- 2 Zookeeper
log "2/9  Zookeeper"
if jps | grep -q QuorumPeerMain; then
  ok "已在运行"
else
  $KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties >/dev/null 2>&1
  for i in $(seq 1 15); do port_up 2181 && break; sleep 2; done
  port_up 2181 && ok "已启动" || bad "2181 未监听，见 $KAFKA_HOME/logs/zookeeper.out"
fi

# ---------------------------------------------------------------- 3 Kafka
log "3/9  Kafka"
if port_up 9092; then
  ok "已在运行"
else
  pkill -f kafka.Kafka 2>/dev/null; sleep 2
  # 上次异常关机 / 强杀会残留 /brokers/ids/0，先删掉再起
  echo "delete /brokers/ids/0" | \
    $KAFKA_HOME/bin/zookeeper-shell.sh localhost:2181 >/dev/null 2>&1 || true
  $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties >/dev/null 2>&1
  for i in $(seq 1 20); do port_up 9092 && break; sleep 2; done
  if port_up 9092; then
    ok "已启动"
  else
    pkill -f kafka.Kafka 2>/dev/null; sleep 3
    echo "deleteall /brokers/ids" | $KAFKA_HOME/bin/zookeeper-shell.sh localhost:2181 >/dev/null 2>&1 || true
    $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties >/dev/null 2>&1
    for i in $(seq 1 20); do port_up 9092 && break; sleep 2; done
    port_up 9092 && ok "已启动（清理残留节点后恢复）" || bad "9092 未监听，见 $KAFKA_HOME/logs/server.log"
  fi
fi

# ---------------------------------------------------------------- 4 HDFS
log "4/9  HDFS（NameNode / DataNode / SecondaryNameNode）"
if jps | grep -q NameNode; then
  ok "已在运行"
else
  $HADOOP_HOME/sbin/start-dfs.sh >/dev/null 2>&1
  for i in $(seq 1 20); do port_up 8020 && break; sleep 2; done
  port_up 8020 && ok "已启动" || bad "8020 未监听"
fi
if timeout 90 hdfs dfsadmin -safemode wait >/dev/null 2>&1; then ok "已退出安全模式"
else bad "安全模式等待超时（可继续，稍后自动恢复）"; fi

# ---------------------------------------------------------------- 5 YARN
log "5/9  YARN（ResourceManager / NodeManager）"
if jps | grep -q ResourceManager; then
  ok "已在运行"
else
  $HADOOP_HOME/sbin/start-yarn.sh >/dev/null 2>&1
  for i in $(seq 1 15); do port_up 8088 && break; sleep 2; done
  port_up 8088 && ok "已启动" || bad "8088 未监听"
fi

# ---------------------------------------------------------------- 6 Hive Metastore
log "6/9  Hive Metastore"
if port_up 9083; then
  ok "已在运行"
else
  nohup $HIVE_HOME/bin/hive --service metastore </dev/null > /tmp/metastore.log 2>&1 &
  disown 2>/dev/null
  for i in $(seq 1 20); do port_up 9083 && break; sleep 2; done
  port_up 9083 && ok "已启动" || bad "9083 未监听，见 /tmp/metastore.log"
fi

# ---------------------------------------------------------------- 7 Flask
log "7/9  Flask 接口（5000）"
pkill -f flask_server.py 2>/dev/null; sleep 1
nohup $PY "$HERE/src/flask_server.py" </dev/null > /tmp/flask.log 2>&1 &
disown 2>/dev/null
for i in $(seq 1 15); do curl -s -m 2 -o /dev/null http://127.0.0.1:5000/api/health && break; sleep 1; done
curl -s -m 2 -o /dev/null http://127.0.0.1:5000/api/health && ok "接口正常" || bad "接口未响应，见 /tmp/flask.log"

# ---------------------------------------------------------------- 8 大屏
log "8/9  大屏静态服务（8000）"
pkill -f "http.server 8000" 2>/dev/null; sleep 1
( cd "$HERE/dashboard" && nohup $PY -m http.server 8000 </dev/null > /tmp/httpd.log 2>&1 & )
disown 2>/dev/null
for i in $(seq 1 10); do curl -s -m 2 -o /dev/null http://127.0.0.1:8000/dashboard.html && break; sleep 1; done
curl -s -m 2 -o /dev/null http://127.0.0.1:8000/dashboard.html && ok "大屏可访问" || bad "8000 未响应，见 /tmp/httpd.log"

# ---------------------------------------------------------------- 9 流任务
log "9/9  Spark 流任务"
if pgrep -f spark_streaming.py >/dev/null; then
  ok "已在运行"
else
  nohup spark-submit --master local[2] "$HERE/src/spark_streaming.py" --mode stream \
        </dev/null > /tmp/stream.log 2>&1 &
  disown 2>/dev/null
  echo -n "    启动中（最多等 60 秒）"
  for i in $(seq 1 30); do
    sleep 2; echo -n "."
    grep -q "流任务已启动" /tmp/stream.log 2>/dev/null && break
  done
  echo
  grep -q "流任务已启动" /tmp/stream.log 2>/dev/null && ok "流任务就绪" || bad "流任务未就绪，见 /tmp/stream.log"
fi

# ---------------------------------------------------------------- 订单回放
if [ "$WITH_PRODUCER" = "1" ]; then
  log "附加：订单回放（$RATE 条/秒）"
  pkill -f kafka_producer.py 2>/dev/null; sleep 1
  nohup $PY "$HERE/src/kafka_producer.py" --mode stream --rate "$RATE" </dev/null > /tmp/producer.log 2>&1 &
  disown 2>/dev/null
  sleep 3
  pgrep -f kafka_producer.py >/dev/null && ok "已启动，数字会持续增长" || bad "生产者没起来，见 /tmp/producer.log"
fi

# ---------------------------------------------------------------- 验收
echo
echo "------------------------------------------------------------------"
echo " 验收"
mysql -uroot -p123456 -N -e "select concat('   实时指标：',order_cnt,' 单 / ',total_amount,' 元 / 异常 ',abnormal_cnt,' 单') from gx_ec_mysql.realtime_order_index;" 2>/dev/null
echo "   Java 进程：$(jps | grep -vc Jps) 个"
curl -s -m 5 -o /dev/null -w "   Flask  /api/health : %{http_code}\n" http://127.0.0.1:5000/api/health
curl -s -m 5 -o /dev/null -w "   大屏   dashboard   : %{http_code}\n" http://127.0.0.1:8000/dashboard.html
echo "------------------------------------------------------------------"
echo " 大屏地址： http://$IP:8000/dashboard.html"
echo " HDFS    ： http://$IP:9870        YARN： http://$IP:8088"
echo "=================================================================="
