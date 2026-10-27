-- ============================================================
-- Hive 四层数仓建表脚本（A组：广西特色农产品电商订单分析）
-- 数据库：gx_ec_hive
-- 分层：ODS-原始层 / DWD-明细层 / DWS-汇总层 / ADS-应用层
-- 说明：在教研室模板基础上做了 3 处扩展（详见 README 第 4 节）
--   1) ODS/DWD 增加 buyer_id 字段，用于用户聚类（RFM）
--   2) 新增 DWD 异常订单表 dwd_eshop_order_abnormal，支撑大屏告警列表
--   3) DWS/ADS 改为按 dt 分区，避免全表扫描（分区裁剪优化点）
-- ============================================================

CREATE DATABASE IF NOT EXISTS gx_ec_hive
COMMENT '广西农产品电商流批一体数仓'
LOCATION '/user/hive/warehouse/gx_ec_hive.db';

USE gx_ec_hive;

-- ------------------------------------------------------------
-- ODS 层：原始订单明细（Kafka 落 HDFS 的 JSON 原样映射）
-- 存储：TEXTFILE + \t 分隔，按日分区，保留全量原始数据便于回溯
-- ------------------------------------------------------------
DROP TABLE IF EXISTS ods_eshop_order;
CREATE EXTERNAL TABLE ods_eshop_order (
    order_id      STRING  COMMENT '订单ID',
    buyer_id      STRING  COMMENT '买家ID（用于用户聚类）',
    product_code  STRING  COMMENT '商品编码',
    product_name  STRING  COMMENT '商品名称',
    category      STRING  COMMENT '品类',
    city          STRING  COMMENT '所属地市',
    order_time    STRING  COMMENT '下单时间 yyyy-MM-dd HH:mm:ss',
    amount        DECIMAL(12,2) COMMENT '订单金额',
    address       STRING  COMMENT '收货地址',
    tag           STRING  COMMENT '数据标记 normal/error'
)
COMMENT 'ODS原始订单表'
PARTITIONED BY (dt STRING COMMENT '分区日期 yyyy-MM-dd')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/gx_ec_hive.db/ods_eshop_order';

-- ------------------------------------------------------------
-- DWD 层：清洗后的有效订单
-- 清洗规则：金额>0、地址非空、订单ID非空且不重复、时间可解析
-- 存储：ORC + Snappy（列存压缩，压缩比约 5:1）
-- ------------------------------------------------------------
DROP TABLE IF EXISTS dwd_eshop_order_valid;
CREATE TABLE dwd_eshop_order_valid (
    order_id      STRING,
    buyer_id      STRING,
    product_code  STRING,
    product_name  STRING,
    category      STRING,
    city          STRING,
    order_time    STRING,
    amount        DECIMAL(12,2),
    address       STRING,
    order_hour    STRING COMMENT '下单小时，DWS聚合用'
)
COMMENT 'DWD有效订单表'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ------------------------------------------------------------
-- DWD 层：异常订单明细（大屏告警列表数据源）
-- ------------------------------------------------------------
DROP TABLE IF EXISTS dwd_eshop_order_abnormal;
CREATE TABLE dwd_eshop_order_abnormal (
    order_id        STRING,
    buyer_id        STRING,
    product_name    STRING,
    category        STRING,
    city            STRING,
    order_time      STRING,
    amount          DECIMAL(12,2),
    address         STRING,
    abnormal_reason STRING COMMENT '异常原因：负金额/空地址/订单ID缺失/重复上报'
)
COMMENT 'DWD异常订单表'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ------------------------------------------------------------
-- DWS 层：按 品类 × 地市 × 小时 聚合
-- ------------------------------------------------------------
DROP TABLE IF EXISTS dws_eshop_hour_stats;
CREATE TABLE dws_eshop_hour_stats (
    hour          STRING COMMENT '小时 HH',
    category      STRING,
    city          STRING,
    order_cnt     BIGINT COMMENT '订单量',
    total_amount  DECIMAL(16,2) COMMENT '成交金额',
    abnormal_cnt  BIGINT COMMENT '异常单量'
)
COMMENT 'DWS小时汇总表'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ------------------------------------------------------------
-- ADS 层：日报表（大屏近5日趋势 + 离线指标）
-- ------------------------------------------------------------
DROP TABLE IF EXISTS ads_eshop_daily_report;
CREATE TABLE ads_eshop_daily_report (
    category         STRING,
    city             STRING,
    day_order_cnt    BIGINT,
    day_total_amount DECIMAL(16,2),
    abnormal_cnt     BIGINT,
    avg_price        DECIMAL(10,2) COMMENT '客单价=成交金额/订单量'
)
COMMENT 'ADS日报表'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ------------------------------------------------------------
-- ADS 层：品类销量预测结果（拓展任务）
-- ------------------------------------------------------------
DROP TABLE IF EXISTS ads_category_forecast;
CREATE TABLE ads_category_forecast (
    category       STRING,
    forecast_date  STRING COMMENT '预测日期',
    forecast_cnt   BIGINT COMMENT '预测销量',
    model_name     STRING COMMENT '模型：LinearRegression',
    mae            DECIMAL(10,2) COMMENT '平均绝对误差'
)
COMMENT 'ADS品类销量预测表'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ------------------------------------------------------------
-- 校验：四层表是否建全
-- ------------------------------------------------------------
SHOW TABLES;
-- 预期 6 张：ads_category_forecast / ads_eshop_daily_report /
--            dwd_eshop_order_abnormal / dwd_eshop_order_valid /
--            dws_eshop_hour_stats / ods_eshop_order
