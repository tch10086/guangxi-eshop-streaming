#!/bin/bash
# ============================================================
# 开机后一键拉起全栈
# 顺序：MySQL → ZK → Kafka → HDFS → YARN → Hive Metastore
#       → Flask 接口 → 大屏静态服务
# 用法：bash scripts/boot_all.sh
# 说明：start_all.sh 只起大数据组件；本脚本额外把 Flask 和
#       http.server 也叫起来，避免"重启后大屏打不开"。
# ============================================================
source /etc/profile.d/bigdata.sh 2>/dev/null

HERE="$(cd "$(dirname "$0")" && pwd)"
PROJ="$(dirname "$HERE")"
cd "$PROJ" || exit 1

echo "===== 1/3 启动大数据组件 ====="
bash "$HERE/start_all.sh" 2>&1 | tail -12

echo "===== 2/3 启动 Flask 接口（5000）====="
pkill -f flask_server.py 2>/dev/null
sleep 1
nohup /opt/module/miniconda3/bin/python "$PROJ/src/flask_server.py" > /tmp/flask.log 2>&1 &
sleep 6

echo "===== 3/3 启动大屏静态服务（8000）====="
pkill -f "http.server 8000" 2>/dev/null
sleep 1
( cd "$PROJ/dashboard" && nohup /opt/module/miniconda3/bin/python -m http.server 8000 > /tmp/httpd.log 2>&1 & )
sleep 3

echo "===== 验收 ====="
curl -s -m 6 -o /dev/null -w "Flask   /api/health : %{http_code}\n" http://127.0.0.1:5000/api/health
curl -s -m 6 -o /dev/null -w "大屏    dashboard   : %{http_code}\n" http://127.0.0.1:8000/dashboard.html
mysql -uroot -p123456 -N -e "select concat('实时指标：',order_cnt,' 单 / ',total_amount,' 元 / 异常 ',abnormal_cnt,' 单') from gx_ec_mysql.realtime_order_index;" 2>/dev/null
jps | grep -vE "Jps" | wc -l | xargs echo "Java 进程数："
echo "大屏地址： http://192.168.100.100:8000/dashboard.html"
