# -*- coding: utf-8 -*-
"""
Kafka 订单生产者（A组：广西特色农产品电商订单分析）

功能：
    持续模拟广西特色农产品线上订单 JSON 数据流，发送到 Kafka 指定 Topic。
    相比教研室模板，增加了 3 个"可演示"能力（对应 35 分核心考点）：

    1. 重复订单号注入  —— 5% 概率复用最近出现过的 order_id 再发一次，
                          用来演示 Structured Streaming 的 dropDuplicates 去重
    2. 迟到数据注入    —— 10% 概率把 order_time 往前推 20~180 秒，
                          用来演示 Watermark(60s) 如何丢弃/接纳迟到数据
    3. 三类异常订单    —— 负金额 / 空地址 / 订单ID缺失，
                          DWD 层清洗后进入异常告警表，大屏告警列表有数据可看

运行：
    python kafka_producer.py                # 持续发送
    python kafka_producer.py --total 500    # 只发 500 条后退出（录屏/截图方便）
    python kafka_producer.py --ip 192.168.100.100 --topic guangxi-eshop-stream
"""

import argparse
import json
import random
import time
from collections import deque
from datetime import datetime, timedelta

from kafka import KafkaProducer

# ==================== 配置区（换环境只改这里） ====================
DEFAULT_IP = "192.168.100.100"          # 改成你自己虚拟机的 IP
DEFAULT_PORT = 9092
DEFAULT_TOPIC = "guangxi-eshop-stream"  # A组固定 Topic，不要改

SEND_INTERVAL = (0.35, 1.0)   # 每条消息间隔（秒），录屏时别太快
ERROR_RATE = 0.06             # 异常订单比例
DUP_RATE = 0.05              # 重复订单号比例（去重演示）
LATE_RATE = 0.10             # 迟到数据比例（水印演示）
LATE_SECONDS = (20, 180)     # 迟到范围（秒），60s 水印会让其中一部分被丢弃

# 广西特色农产品池：编码 / 名称 / 品类 / 产地地市 / 权重
PRODUCTS = [
    {"code": "P001", "name": "砂糖橘",       "category": "水果生鲜", "city": "桂林",   "weight": 22},
    {"code": "P002", "name": "武鸣沃柑",     "category": "水果生鲜", "city": "南宁",   "weight": 20},
    {"code": "P003", "name": "百色芒果",     "category": "水果生鲜", "city": "百色",   "weight": 18},
    {"code": "P004", "name": "北流百香果",   "category": "水果生鲜", "city": "玉林",   "weight": 12},
    {"code": "P005", "name": "柳州螺蛳粉",   "category": "速食特产", "city": "柳州",   "weight": 15},
    {"code": "P006", "name": "横州茉莉花茶", "category": "茶叶干货", "city": "南宁",   "weight": 6},
    {"code": "P007", "name": "荔浦芋头",     "category": "蔬菜菌菇", "city": "桂林",   "weight": 4},
    {"code": "P008", "name": "融安金桔",     "category": "水果生鲜", "city": "柳州",   "weight": 3},
]

# 地市池（用于收货地址随机生成）
ADDRESS_TEMPLATE = "{city}市{city}区{district}大道{num}号{unit}栋{room}室"

DISTRICTS = {
    "桂林": "象山", "南宁": "青秀", "百色": "右江",
    "玉林": "玉州", "柳州": "城中",
}


def build_producer(ip: str, port: int) -> KafkaProducer:
    """创建 Kafka 生产者，value 用 JSON 序列化（中文不转义）"""
    return KafkaProducer(
        bootstrap_servers=[f"{ip}:{port}"],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",              # 等所有副本确认，演示时不丢消息
        linger_ms=50,            # 攒批 50ms，降低网络往返
        retries=3,
    )


def pick_product() -> dict:
    """按权重随机挑选商品，让销量分布更接近真实（砂糖橘/沃柑卖得多）"""
    weights = [p["weight"] for p in PRODUCTS]
    return random.choices(PRODUCTS, weights=weights, k=1)[0]


def make_address(city: str) -> str:
    """生成像样的收货地址"""
    district = DISTRICTS.get(city, city)
    return ADDRESS_TEMPLATE.format(
        city=city, district=district,
        num=random.randint(1, 199),
        unit=random.randint(1, 20),
        room=random.randint(101, 2508),
    )


def build_order(recent_ids: deque) -> tuple:
    """
    构造一条订单记录，返回 (order_dict, is_abnormal, dup_flag)
    按概率依次决定：是否异常 -> 是否重复单号 -> 是否迟到
    """
    product = pick_product()
    now = datetime.now()

    # ---------- 1) 正常字段 ----------
    order_id = "OD{}{:06d}".format(now.strftime("%Y%m%d%H%M%S"), random.randint(0, 999999))
    buyer_id = "BH{:05d}".format(random.randint(1, 500))   # 500 个买家，够做聚类
    amount = round(random.uniform(9.9, 328.0), 2)
    address = make_address(product["city"])
    order_time = now
    tag = "normal"
    is_abnormal = False

    # ---------- 2) 异常注入（6%）----------
    if random.random() < ERROR_RATE:
        is_abnormal = True
        tag = "error"
        abnormal_type = random.choice(["negative_amount", "empty_address", "missing_id"])
        if abnormal_type == "negative_amount":
            amount = -round(random.uniform(1.0, 50.0), 2)
        elif abnormal_type == "empty_address":
            address = ""
        else:
            order_id = None          # 订单ID缺失
            amount = round(random.uniform(9.9, 99.0), 2)   # 金额其实正常，只是ID没了

    # ---------- 3) 重复订单号注入（5%，去重演示用）----------
    dup_flag = False
    if recent_ids and random.random() < DUP_RATE:
        order_id = random.choice(recent_ids)
        dup_flag = True               # 同一个订单号再发一次，dropDuplicates 要去掉它

    # ---------- 4) 迟到数据注入（10%，水印演示用）----------
    late_flag = False
    if random.random() < LATE_RATE:
        order_time = now - timedelta(seconds=random.randint(*LATE_SECONDS))
        late_flag = True

    order = {
        "order_id": order_id,
        "buyer_id": buyer_id,
        "product_code": product["code"],
        "product_name": product["name"],
        "category": product["category"],
        "city": product["city"],
        "order_time": order_time.strftime("%Y-%m-%d %H:%M:%S"),
        "amount": amount,
        "address": address,
        "tag": tag,
    }

    # 记录本条的订单号，供后续重复注入挑选
    if order_id:
        recent_ids.append(order_id)

    return order, is_abnormal, dup_flag, late_flag


def main():
    parser = argparse.ArgumentParser(description="广西农产品电商订单 Kafka 生产者")
    parser.add_argument("--ip", default=DEFAULT_IP, help="Kafka 所在机器 IP")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--total", type=int, default=0, help="发送总条数，0 表示不限")
    args = parser.parse_args()

    producer = build_producer(args.ip, args.port)
    recent_ids = deque(maxlen=300)   # 只记住最近 300 个订单号

    sent = abnormal = duplicated = late = 0
    print("=" * 78)
    print("A组电商 Kafka 生产者启动")
    print("Kafka: {}:{}   Topic: {}".format(args.ip, args.port, args.topic))
    print("异常率 {}%  重复单号 {}%  迟到数据 {}%".format(
        int(ERROR_RATE * 100), int(DUP_RATE * 100), int(LATE_RATE * 100)))
    print("=" * 78)

    try:
        while True:
            order, is_abnormal, dup_flag, late_flag = build_order(recent_ids)
            producer.send(args.topic, value=order)

            sent += 1
            abnormal += int(is_abnormal)
            duplicated += int(dup_flag)
            late += int(late_flag)

            # 每 20 条打印一次概况，避免刷屏
            if sent % 20 == 0:
                producer.flush()
                print("[{:>5}条] 异常{} 重复{} 迟到{} | 最新: {} {} ¥{} {}".format(
                    sent, abnormal, duplicated, late,
                    order["order_id"], order["product_name"],
                    order["amount"], order["order_time"]))
            else:
                print(json.dumps(order, ensure_ascii=False))

            if args.total and sent >= args.total:
                break
            time.sleep(random.uniform(*SEND_INTERVAL))

    except KeyboardInterrupt:
        print("\n收到 Ctrl+C，正在刷盘退出…")
    finally:
        producer.flush()
        producer.close()
        print("-" * 78)
        print("共发送 {} 条 | 异常 {} 条 | 重复单号 {} 条 | 迟到 {} 条".format(
            sent, abnormal, duplicated, late))


if __name__ == "__main__":
    main()
