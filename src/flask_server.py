# -*- coding: utf-8 -*-
"""
Flask 后端接口（A组：广西特色农产品电商订单分析）

接口列表：
    GET /api/eshop_data        大屏主接口（实时指标 + 离线趋势 + 品类/地市 + 告警 + 预测）
    GET /api/abnormal_orders   异常订单分页查询
    GET /api/forecast          品类销量预测结果
    GET /api/health            健康检查（部署验收用）

相比教研室模板修复了 2 个必踩的坑：
    1. pymysql 查 DECIMAL 返回 decimal.Decimal，直接 jsonify 会抛
       TypeError: Object of type Decimal is not JSON serializable
       —— 本文件统一用 to_jsonable() 递归转换
    2. 模板只查 realtime/ads/dws 三张表，但 dws 表在 MySQL 里并不存在
       —— 本文件改为优先查实时表 realtime_category_index / realtime_city_index，
          查不到再回退到 dws 表，不会因为缺表直接 500

启动：
    python flask_server.py            # 默认 0.0.0.0:5000
    python flask_server.py --port 5000 --debug
"""

import argparse
from datetime import date, datetime
from decimal import Decimal

import pymysql
from flask import Flask, jsonify, request
from flask_cors import CORS

# ==================== 数据库配置，按自己环境修改 ====================
DB_CONFIG = {
    "host": "192.168.100.100",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "database": "gx_ec_mysql",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

app = Flask(__name__)
CORS(app)                      # 允许大屏跨域访问
app.json.ensure_ascii = False  # 中文不转义，方便直接看 JSON


# ==================== 工具函数 ====================
def to_jsonable(obj):
    """
    递归把 MySQL 返回的特殊类型转成 JSON 可序列化类型。
    这是模板里最典型的 500 错误来源，单独抽出来统一处理。
    """
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.strftime("%Y-%m-%d %H:%M:%S") if isinstance(obj, datetime) \
            else obj.strftime("%Y-%m-%d")
    if isinstance(obj, dict):
        return {k: to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    return obj


class Db:
    """with Db() as cur: 的简易上下文管理器，保证连接一定被关闭"""
    def __enter__(self):
        self.conn = pymysql.connect(**DB_CONFIG)
        self.cur = self.conn.cursor()
        return self.cur

    def __exit__(self, exc_type, exc, tb):
        try:
            self.cur.close()
            self.conn.close()
        except Exception:
            pass
        return False


def query_all(sql, args=None):
    with Db() as cur:
        cur.execute(sql, args or ())
        return cur.fetchall()


def query_one(sql, args=None):
    with Db() as cur:
        cur.execute(sql, args or ())
        return cur.fetchone()


# ==================== 主接口 ====================
@app.route("/api/eshop_data", methods=["GET"])
def eshop_data():
    try:
        # ---- 1) 实时累计指标 ----
        rt = query_one(
            """
            SELECT order_cnt, total_amount, abnormal_cnt, avg_price, order_time
            FROM realtime_order_index ORDER BY order_time DESC LIMIT 1
            """) or {"order_cnt": 0, "total_amount": 0, "abnormal_cnt": 0,
                     "avg_price": 0, "order_time": None}

        # ---- 2) 近5天离线成交趋势（ADS 日报表）----
        day_rows = query_all(
            """
            SELECT dt,
                   SUM(day_order_cnt)    AS order_cnt,
                   SUM(day_total_amount) AS amount,
                   SUM(abnormal_cnt)     AS abnormal
            FROM ads_eshop_daily_report
            GROUP BY dt ORDER BY dt DESC LIMIT 5
            """)
        day_rows = list(reversed(day_rows))     # 反转成时间正序

        # ---- 3) 品类销量排行：优先实时表，回退 DWS ----
        cat_rows = query_all(
            "SELECT category AS name, order_cnt AS value FROM realtime_category_index "
            "ORDER BY value DESC")
        if not cat_rows:
            cat_rows = query_all(
                "SELECT category AS name, SUM(order_cnt) AS value "
                "FROM dws_eshop_hour_stats GROUP BY category ORDER BY value DESC")

        # ---- 4) 地市订单分布：优先实时表，回退 DWS ----
        city_rows = query_all(
            "SELECT city AS name, order_cnt AS value FROM realtime_city_index "
            "ORDER BY value DESC")
        if not city_rows:
            city_rows = query_all(
                "SELECT city AS name, SUM(order_cnt) AS value "
                "FROM dws_eshop_hour_stats GROUP BY city ORDER BY value DESC")

        # ---- 5) 最近 2 小时的分钟级成交趋势（实时）----
        # 不能用 "ORDER BY stat_minute DESC LIMIT 30"：停了几天再跑，会把上周的分钟
        # 和今天的混在一条线里，横轴时间倒退。这里以最新一条为基准往前取 2 小时。
        minute_rows = query_all(
            """
            SELECT stat_minute, order_cnt, total_amount
            FROM realtime_order_minute
            WHERE stat_minute >= (SELECT MAX(stat_minute) FROM realtime_order_minute) - INTERVAL 2 HOUR
            ORDER BY stat_minute
            """)

        # ---- 6) 最新 10 条异常订单（大屏告警列表）----
        abnormal_rows = query_all(
            """
            SELECT order_id, product_name, city, amount, abnormal_reason, order_time
            FROM abnormal_order_log ORDER BY id DESC LIMIT 10
            """)

        # ---- 7) 预测结果（拓展任务，没有就返回空，不影响大屏）----
        try:
            # 只取最新基准日那一批，避免历史快照混进来
            forecast_rows = query_all(
                """
                SELECT category, forecast_date, forecast_cnt
                FROM ads_category_forecast
                WHERE dt = (SELECT MAX(dt) FROM ads_category_forecast)
                ORDER BY forecast_date, category
                """)
        except Exception:
            forecast_rows = []

        resp = {
            "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "realtime": rt,
            "dayTrend": {
                "x": [r["dt"] for r in day_rows],
                "y": [float(r["amount"] or 0) for r in day_rows],
                "abnormal": [int(r["abnormal"] or 0) for r in day_rows],
                "orderCnt": [int(r["order_cnt"] or 0) for r in day_rows],
            },
            "category": cat_rows,
            "area": city_rows,
            "minuteTrend": {
                "x": [r["stat_minute"].strftime("%H:%M") if r["stat_minute"] else ""
                      for r in minute_rows],
                "y": [float(r["total_amount"] or 0) for r in minute_rows],
            },
            "abnormal": abnormal_rows,
            "forecast": forecast_rows,
        }
        return jsonify(to_jsonable(resp))

    except Exception as e:
        # 把真实错误抛回去，方便部署阶段定位（上线时可关闭）
        return jsonify({
            "error": str(e),
            "hint": "检查 Flask 的 DB_CONFIG 与 MySQL 是否已执行 mysql_ddl.sql",
        }), 500


@app.route("/api/abnormal_orders", methods=["GET"])
def abnormal_orders():
    """异常订单分页查询"""
    page = max(int(request.args.get("page", 1)), 1)
    size = min(max(int(request.args.get("size", 20)), 1), 200)
    offset = (page - 1) * size
    rows = query_all(
        """
        SELECT id, order_id, buyer_id, product_name, city, amount,
               abnormal_reason, order_time
        FROM abnormal_order_log ORDER BY id DESC LIMIT %s OFFSET %s
        """, (size, offset))
    total = query_one("SELECT COUNT(*) AS c FROM abnormal_order_log")["c"]
    return jsonify(to_jsonable({
        "page": page, "size": size, "total": total, "rows": rows,
    }))


@app.route("/api/forecast", methods=["GET"])
def forecast():
    rows = query_all(
        "SELECT dt, category, forecast_date, forecast_cnt, model_name, mae "
        "FROM ads_category_forecast ORDER BY forecast_date, category")
    return jsonify(to_jsonable({"rows": rows}))


@app.route("/api/health", methods=["GET"])
def health():
    """部署验收：能返回 ok 说明 Flask 与 MySQL 已打通"""
    try:
        one = query_one("SELECT 1 AS ok")
        tables = query_all("SHOW TABLES")
        return jsonify({
            "status": "ok",
            "db": DB_CONFIG["database"],
            "tables": [list(t.values())[0] for t in tables],
            "check": one["ok"],
        })
    except Exception as e:
        return jsonify({"status": "fail", "error": str(e)}), 500


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    print("A组电商 Flask 接口启动：http://{}:{}/api/eshop_data"
          .format(args.host, args.port))
    app.run(host=args.host, port=args.port, debug=args.debug)
