-- ============================================================
-- MySQL 业务库建表脚本（A组：广西特色农产品电商订单分析）
-- 作用：1) 承接 Structured Streaming 实时指标（foreachBatch 写入）
--       2) 承接 SparkSQL 离线 ADS/DWS 指标（供大屏离线板块）
--       3) 承接异常告警明细与预测结果
-- 说明：教研室资源包未提供 MySQL 建表脚本，本文件为补齐项
-- ============================================================

CREATE DATABASE IF NOT EXISTS gx_ec_mysql
    DEFAULT CHARACTER SET utf8mb4
    COLLATE utf8mb4_general_ci;

USE gx_ec_mysql;

-- ------------------------------------------------------------
-- 1) 实时指标总表（单行累计，id 恒为 1）
--    Structured Streaming 每微批 UPSERT 累加，Flask 取最新一行
-- ------------------------------------------------------------
DROP TABLE IF EXISTS realtime_order_index;
CREATE TABLE realtime_order_index (
    id            TINYINT      NOT NULL DEFAULT 1 COMMENT '固定主键，保证单行',
    order_cnt     BIGINT       NOT NULL DEFAULT 0 COMMENT '累计订单量',
    total_amount  DECIMAL(16,2) NOT NULL DEFAULT 0.00 COMMENT '累计成交金额',
    abnormal_cnt  BIGINT       NOT NULL DEFAULT 0 COMMENT '累计异常订单数',
    avg_price     DECIMAL(10,2) NOT NULL DEFAULT 0.00 COMMENT '客单价=成交金额/订单量',
    order_time    DATETIME     NOT NULL COMMENT '最近一次更新对应的业务时间',
    update_time   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='实时累计指标表';

-- 初始化单行，避免大屏首次查询为空
INSERT INTO realtime_order_index (id, order_cnt, total_amount, abnormal_cnt, avg_price, order_time)
VALUES (1, 0, 0.00, 0, 0.00, NOW())
ON DUPLICATE KEY UPDATE id = id;

-- ------------------------------------------------------------
-- 2) 实时分钟趋势（大屏"今日成交趋势"用，按分钟累加）
-- ------------------------------------------------------------
DROP TABLE IF EXISTS realtime_order_minute;
CREATE TABLE realtime_order_minute (
    stat_minute  DATETIME     NOT NULL COMMENT '分钟时间点',
    order_cnt    BIGINT       NOT NULL DEFAULT 0,
    total_amount DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    PRIMARY KEY (stat_minute)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='实时分钟级趋势表';

-- ------------------------------------------------------------
-- 3) 实时品类排行 / 实时地市分布（大屏实时板块用，流任务累加）
-- ------------------------------------------------------------
DROP TABLE IF EXISTS realtime_category_index;
CREATE TABLE realtime_category_index (
    category     VARCHAR(32)   NOT NULL,
    order_cnt    BIGINT        NOT NULL DEFAULT 0,
    total_amount DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    update_time  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='实时品类排行表';

DROP TABLE IF EXISTS realtime_city_index;
CREATE TABLE realtime_city_index (
    city         VARCHAR(32)   NOT NULL,
    order_cnt    BIGINT        NOT NULL DEFAULT 0,
    total_amount DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    update_time  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (city)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='实时地市分布表';

-- ------------------------------------------------------------
-- 4) DWS 小时汇总（离线 ETL 同步，品类/地市排行榜数据源）
-- ------------------------------------------------------------
DROP TABLE IF EXISTS dws_eshop_hour_stats;
CREATE TABLE dws_eshop_hour_stats (
    dt           VARCHAR(10)   NOT NULL COMMENT '日期 yyyy-MM-dd',
    hour         VARCHAR(2)    NOT NULL COMMENT '小时 HH',
    category     VARCHAR(32)   NOT NULL,
    city         VARCHAR(32)   NOT NULL,
    order_cnt    BIGINT        NOT NULL DEFAULT 0,
    total_amount DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    abnormal_cnt BIGINT        NOT NULL DEFAULT 0,
    PRIMARY KEY (dt, hour, category, city),
    KEY idx_category (category),
    KEY idx_city (city)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS小时汇总表';

-- ------------------------------------------------------------
-- 5) ADS 日报表（近5日成交趋势数据源）
-- ------------------------------------------------------------
DROP TABLE IF EXISTS ads_eshop_daily_report;
CREATE TABLE ads_eshop_daily_report (
    dt               VARCHAR(10)   NOT NULL,
    category         VARCHAR(32)   NOT NULL,
    city             VARCHAR(32)   NOT NULL,
    day_order_cnt    BIGINT        NOT NULL DEFAULT 0,
    day_total_amount DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    abnormal_cnt     BIGINT        NOT NULL DEFAULT 0,
    avg_price        DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    PRIMARY KEY (dt, category, city),
    KEY idx_dt (dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS日报表';

-- ------------------------------------------------------------
-- 6) 异常订单告警明细（大屏告警列表 + 截图证据）
-- ------------------------------------------------------------
DROP TABLE IF EXISTS abnormal_order_log;
CREATE TABLE abnormal_order_log (
    id              BIGINT       NOT NULL AUTO_INCREMENT,
    order_id        VARCHAR(64)  DEFAULT NULL,
    buyer_id        VARCHAR(32)  DEFAULT NULL,
    product_name    VARCHAR(64)  DEFAULT NULL,
    category        VARCHAR(32)  DEFAULT NULL,
    city            VARCHAR(32)  DEFAULT NULL,
    amount          DECIMAL(12,2) DEFAULT NULL,
    address         VARCHAR(255) DEFAULT NULL,
    abnormal_reason VARCHAR(64)  DEFAULT NULL COMMENT '负金额/空地址/订单ID缺失/重复上报',
    order_time      DATETIME     DEFAULT NULL,
    create_time     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_create_time (create_time),
    KEY idx_order_id (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='异常订单告警表';

-- ------------------------------------------------------------
-- 7) 品类销量预测结果（拓展任务）
-- ------------------------------------------------------------
DROP TABLE IF EXISTS ads_category_forecast;
CREATE TABLE ads_category_forecast (
    dt            VARCHAR(10) NOT NULL COMMENT '训练数据截止日期',
    category      VARCHAR(32) NOT NULL,
    forecast_date VARCHAR(10) NOT NULL COMMENT '预测日期',
    forecast_cnt  BIGINT      NOT NULL DEFAULT 0,
    model_name    VARCHAR(32) DEFAULT 'LinearRegression',
    mae           DECIMAL(10,2) DEFAULT NULL,
    PRIMARY KEY (dt, category, forecast_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='品类销量预测表';

-- ------------------------------------------------------------
-- 校验
-- ------------------------------------------------------------
SHOW TABLES;
-- 预期 6 张表

SELECT 'real-time row ready' AS check_result, COUNT(*) AS rows_in_realtime
FROM realtime_order_index;
