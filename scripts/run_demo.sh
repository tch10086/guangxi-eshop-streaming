#!/bin/bash
# ============================================================
# 一键恢复"演示态"：组件 → 流任务 → 生产者（可选再跑离线 ETL）
# 用法：
#   bash scripts/run_demo.sh              # 起流任务 + 发 300 条订单
#   bash scripts/run_demo.sh 500          # 发 500 条
#   bash scripts/run_demo.sh 300 --etl    # 发完再补跑今天的离线 ETL
# 场景：机器重启、隔天再打开大屏时，实时数字不动了，跑这个脚本即可恢复。
# ============================================================
source /etc/profile.d/bigdata.sh 2>/dev/null
export PYSPARK_PYTHON=/opt/module/miniconda3/bin/python

HERE="$(cd "$(dirname "$0")" && pwd)"
PROJ="$(dirname "$HERE")"
cd "$PROJ" || exit 1

TOTAL=${1:-300}
DO_ETL=${2:-}

echo "===== 1/4 检查基础组件 ====="
if ! jps | grep -q Kafka; then
  echo "Kafka 未启动，先执行 start_all.sh"
  bash "$HERE/start_all.sh" 2>&1 | tail -6
  sleep 10
fi
jps | grep -E "Kafka|NameNode|ResourceManager" | awk '{print "  " $2}'

echo "===== 2/4 启动流任务 ====="
if pgrep -f spark_streaming.py > /dev/null; then
  echo "  流任务已在运行"
else
  nohup spark-submit --master local[2] src/spark_streaming.py --mode stream > /tmp/stream.log 2>&1 &
  echo "  已提交，等待 Spark 初始化（约 50 秒）…"
  sleep 55
  grep -E "流任务已启动|ERROR" /tmp/stream.log | tail -2
fi

echo "===== 3/4 发送订单（$TOTAL 条）====="
if pgrep -f kafka_producer.py > /dev/null; then
  echo "  生产者已在运行"
else
  nohup /opt/module/miniconda3/bin/python src/kafka_producer.py \
      --ip 192.168.100.100 --total "$TOTAL" > /tmp/producer.log 2>&1 &
  echo "  已启动，等待写入…"
fi
sleep 90

echo "===== 4/4 当前指标 ====="
mysql -uroot -p123456 -N -e \
  "select concat('  实时：',order_cnt,' 单 / ',total_amount,' 元 / 异常 ',abnormal_cnt,' 单 / 客单价 ',avg_price,' / 更新于 ',update_time) from gx_ec_mysql.realtime_order_index;" 2>/dev/null
mysql -uroot -p123456 -N -e \
  "select concat('  最新分钟：',max(stat_minute),'（共 ',count(*),' 个分钟点）') from gx_ec_mysql.realtime_order_minute;" 2>/dev/null

if [ "$DO_ETL" = "--etl" ]; then
  TODAY=$(date +%F)
  echo "===== 补跑离线 ETL：$TODAY ====="
  spark-submit --master local[2] src/offline_etl.py --dt "$TODAY" 2>&1 \
    | grep -E "处理分区|同步 MySQL|完成" | tail -4
  echo "  重跑预测…"
  /opt/module/miniconda3/bin/python src/ml_forecast.py --days 3 2>&1 | grep -E "已写入" | tail -1
fi

echo
echo "大屏地址： http://192.168.100.100:8000/dashboard.html"
echo "若页面还是旧数字，按 Ctrl+F5 强制刷新。"
