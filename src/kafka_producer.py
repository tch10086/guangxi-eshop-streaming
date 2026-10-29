# -*- coding: utf-8 -*-
"""
柑橘订单回放器（A组：广西特色农产品电商订单分析）

数据来源（真实）：
    北京新发地批发市场公开价格接口 —— 沃柑 / 耙耙柑 / 脐橙 / 砂糖橘 的每日批发价。
    清洗后落在 data/clean/daily_price.csv（116 条 品类×日 记录，2024-03-01~03-31）。

本脚本做的事：
    1. 读真实日价格表（价格不做任何编造）；
    2. 订单金额 = 当日真实均价 × 件重（3/5/10/20 斤）；
    3. 订单量按品类权重、价格弹性、日波动构造；地市按广西柑橘主产区权重分配；
    4. 按任务书规则注入异常单、重复单号、迟到数据，供去重与水印演示。

两种模式：
    python kafka_producer.py --mode stream [--rate 12]
        把最后一天（已映射到今天）的订单压到当前时间发往 Kafka，供实时大屏演示
    python kafka_producer.py --mode batch --out data/orders
        把窗口内前 30 天订单写成 TSV（dt= 分区目录），供 HDFS 离线数仓使用

日期说明：
    2024-03 的真实行情整体平移到最近 30 天，便于大屏显示"近期"；价格数值未修改。
"""

import argparse
import csv
import json
import os
import random
import time
from collections import deque
from datetime import datetime, timedelta

# ==================== 配置 ====================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PRICE_CSV = os.path.join(BASE_DIR, "data", "clean", "daily_price.csv")

KAFKA_IP = "192.168.100.100"
KAFKA_PORT = 9092
TOPIC = "guangxi-eshop-stream"

ORDERS_PER_VARIETY_DAY = 110      # 每品类每天基础单量
VARIETY_WEIGHT = {"沃柑": 1.25, "耙耙柑": 1.00, "脐橙": 0.90, "砂糖橘": 0.75}
BASE_PRICE = 3.60                 # 价格弹性参考价（元/斤）

ERROR_RATE = 0.04                 # 异常单
DUP_RATE = 0.03                   # 重复单号
LATE_RATE = 0.06                  # 迟到数据（水印演示）
LATE_SECONDS = (20, 180)

# 广西 14 个地市，权重按柑橘主产区分布近似（桂林、南宁为主产区）
CITIES = [
    ("桂林", 0.22), ("南宁", 0.18), ("柳州", 0.10), ("贺州", 0.09),
    ("梧州", 0.07), ("玉林", 0.07), ("来宾", 0.06), ("百色", 0.06),
    ("贵港", 0.05), ("钦州", 0.04), ("崇左", 0.03), ("河池", 0.02),
    ("北海", 0.008), ("防城港", 0.002),
]
DISTRICTS = {
    "桂林": ["象山区", "秀峰区", "临桂区", "阳朔县", "荔浦市"],
    "南宁": ["青秀区", "西乡塘区", "武鸣区", "江南区", "横州市"],
    "柳州": ["城中区", "鱼峰区", "柳江区", "融安县"],
    "贺州": ["八步区", "钟山县", "富川县"],
    "梧州": ["万秀区", "长洲区", "岑溪市"],
    "玉林": ["玉州区", "北流市", "容县"],
    "来宾": ["兴宾区", "象州县", "武宣县"],
    "百色": ["右江区", "田阳区", "靖西市"],
    "贵港": ["港北区", "桂平市", "平南县"],
    "钦州": ["钦南区", "灵山县", "浦北县"],
    "崇左": ["江州区", "扶绥县", "凭祥市"],
    "河池": ["金城江区", "宜州区", "南丹县"],
    "北海": ["海城区", "合浦县"],
    "防城港": ["港口区", "东兴市"],
}
PIECE_WEIGHTS = [(3, 0.15), (5, 0.40), (10, 0.30), (20, 0.15)]   # 件重（斤）及占比


def load_prices():
    """读真实日价格表，返回 {日期: {品类: (均价, 编码)}}"""
    table = {}
    with open(PRICE_CSV, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            d = datetime.strptime(r["dt"], "%Y-%m-%d").date()
            table.setdefault(d, {})[r["category"]] = (float(r["avg_price"]), r["product_code"])
    return table


def shift_days(table):
    """把价格窗口整体平移到最近 30 天"""
    end = max(table)
    shift = (datetime.now().date() - end).days
    moved = {}
    for d, v in table.items():
        moved[d + timedelta(days=shift)] = (d, v)
    return shift, moved


def pick_city(rnd):
    r = rnd.random()
    acc = 0.0
    for name, w in CITIES:
        acc += w
        if r <= acc:
            return name
    return CITIES[0][0]


def make_address(city, rnd):
    dist = rnd.choice(DISTRICTS.get(city, ["城区"]))
    return "%s市%s%s路%d号%d栋%d室" % (
        city, dist, rnd.choice(["民主", "解放", "建设", "中山", "人民"]),
        rnd.randint(1, 299), rnd.randint(1, 20), rnd.randint(101, 2508))


def gen_day_orders(new_dt, prices, seq_start, rnd):
    """生成某一天的全部订单；金额严格来自真实单价"""
    orders = []
    seq = seq_start
    for cat, (price, code) in sorted(prices.items()):
        w = VARIETY_WEIGHT.get(cat, 1.0)
        elasticity = (BASE_PRICE / price) ** 0.4          # 价高则销量略降
        noise = rnd.uniform(0.82, 1.18)
        n = max(30, int(ORDERS_PER_VARIETY_DAY * w * elasticity * noise))

        for _ in range(n):
            seq += 1
            hour = rnd.choices([8, 9, 10, 11, 13, 14, 15, 16, 18, 19, 20, 21],
                               weights=[4, 9, 12, 9, 6, 9, 10, 8, 6, 8, 7, 4])[0]
            ts = datetime(new_dt.year, new_dt.month, new_dt.day, hour,
                          rnd.randint(0, 59), rnd.randint(0, 59))
            jin = rnd.choices([p for p, _ in PIECE_WEIGHTS],
                              weights=[x for _, x in PIECE_WEIGHTS])[0]
            amount = round(price * jin, 2)                 # 真实单价 × 件重
            city = pick_city(rnd)
            tag = "normal"
            order_id = "OD%s%06d" % (ts.strftime("%Y%m%d"), seq)
            address = make_address(city, rnd)

            if rnd.random() < ERROR_RATE:                  # 异常注入（任务书三条规则）
                tag = "error"
                kind = rnd.choice(["neg", "empty", "noid"])
                if kind == "neg":
                    amount = -round(rnd.uniform(5, 60), 2)
                elif kind == "empty":
                    address = ""
                else:
                    order_id = None

            orders.append({
                "order_id": order_id, "product_code": code, "product_name": cat,
                "category": cat, "city": city,
                "order_time": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "amount": amount, "address": address, "tag": tag,
                "buyer_id": "BH%05d" % rnd.randint(1, 800),
            })
    orders.sort(key=lambda o: o["order_time"])
    return orders, seq


def build_all(moved):
    result, seq = {}, 0
    for new_dt in sorted(moved):
        old_dt, prices = moved[new_dt]
        rnd = random.Random(abs(hash(old_dt.isoformat())) % (2 ** 31))
        orders, seq = gen_day_orders(new_dt, prices, seq, rnd)
        result[new_dt] = orders
    return result


def write_batch(days, out_dir):
    for dt in sorted(days):
        d = os.path.join(out_dir, "dt=%s" % dt.isoformat())
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "orders.tsv"), "w", encoding="utf-8", newline="\n") as f:
            for o in days[dt]:
                f.write("\t".join([
                    o["order_id"] or "\\N", o["buyer_id"], o["product_code"], o["product_name"],
                    o["category"], o["city"], o["order_time"], "%.2f" % o["amount"],
                    o["address"] if o["address"] else "\\N", o["tag"],
                ]) + "\n")
        print("  %s  %4d 单" % (dt, len(days[dt])))


def stream_day(orders, rate):
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=["%s:%d" % (KAFKA_IP, KAFKA_PORT)],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
        acks="all", linger_ms=30, retries=3)

    print("=" * 74)
    print("柑橘订单回放  沃柑/耙耙柑/脐橙/砂糖橘  %d 单  速率 %d 条/秒" % (len(orders), rate))
    print("Kafka %s:%d   Topic: %s" % (KAFKA_IP, KAFKA_PORT, TOPIC))
    print("=" * 74)

    rnd = random.Random(7)
    recent = deque(maxlen=200)
    sent = abnormal = dup = late = 0
    try:
        for o in orders:
            now = datetime.now()
            t = now.replace(hour=int(o["order_time"][11:13]),
                            minute=int(o["order_time"][14:16]),
                            second=min(int(o["order_time"][17:19]), 59))
            if t > now:
                t = now
            if rnd.random() < LATE_RATE:                   # 迟到：事件时间回拨
                t -= timedelta(seconds=rnd.randint(*LATE_SECONDS))
                late += 1

            payload = dict(o)
            payload["order_time"] = t.strftime("%Y-%m-%d %H:%M:%S")
            if recent and rnd.random() < DUP_RATE:         # 重复单号
                payload["order_id"] = rnd.choice(recent)
                dup += 1
            elif payload["order_id"]:
                recent.append(payload["order_id"])
            if payload["tag"] == "error":
                abnormal += 1

            producer.send(TOPIC, value=payload)
            sent += 1
            if sent % 25 == 0:
                producer.flush()
                print("[%4d] 异常 %d  重复 %d  迟到 %d | %s %s ¥%.2f %s" % (
                    sent, abnormal, dup, late, payload["product_name"],
                    payload["city"], payload["amount"], payload["order_time"]))
            time.sleep(1.0 / max(rate, 1))
    except KeyboardInterrupt:
        print("\n手动中断")
    finally:
        producer.flush()
        producer.close()
        print("-" * 74)
        print("共发送 %d 单 | 异常 %d | 重复单号 %d | 迟到 %d" % (sent, abnormal, dup, late))


def main():
    ap = argparse.ArgumentParser(description="柑橘订单回放器")
    ap.add_argument("--mode", choices=["stream", "batch"], default="stream")
    ap.add_argument("--rate", type=int, default=12, help="流模式每秒发送条数")
    ap.add_argument("--out", default=os.path.join(BASE_DIR, "data", "orders"))
    args = ap.parse_args()

    table = load_prices()
    if not table:
        raise SystemExit("价格表为空，请先运行 tools/build_price_table.py")

    shift, moved = shift_days(table)
    print("真实价格窗口 %s ~ %s，整体平移 %+d 天" % (
        min(table), max(table), shift))

    days = build_all(moved)
    total = sum(len(v) for v in days.values())
    print("生成 %d 天 / %d 单（单价全部来自新发地真实报价）" % (len(days), total))

    if args.mode == "batch":
        write_batch({d: days[d] for d in sorted(days)[:-1]}, args.out)
        print("离线 TSV 已写入 %s（最后一天留给实时流）" % args.out)
    else:
        stream_day(days[max(days)], args.rate)


if __name__ == "__main__":
    main()
