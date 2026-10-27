#!/bin/bash
# ============================================================
# 大数据实训环境一键安装脚本（A组电商 / 伪分布式单机）
#
# 适用：CentOS 7.9 x64（Minimal 亦可）、root 执行
# 版本：JDK1.8.0_202 / Hadoop3.3.4 / Hive3.1.2 /
#       Spark3.3.0(bin-hadoop3) / Kafka3.2.1 / MySQL8.0 / Python3.9
#
# 前置：把下载好的安装包放到 /opt/software/
#   jdk-8u202-linux-x64.tar.gz
#   hadoop-3.3.4.tar.gz
#   apache-hive-3.1.2-bin.tar.gz
#   spark-3.3.0-bin-hadoop3.tgz
#   kafka_2.12-3.2.1.tgz
#   Miniconda3-py39_*.sh
#   mysql-connector-j-8.0.33.jar
#   spark-sql-kafka-0-10_2.12-3.3.0.jar
#   spark-token-provider-kafka-0-10_2.12-3.3.0.jar
#   kafka-clients-3.2.1.jar
#   commons-pool2-2.11.0.jar
#   mysql-8.0.46-1.el7.x86_64.rpm-bundle.tar   （可选，已装 MySQL 可跳过）
#
# 用法：bash install_env.sh
# ============================================================
set -euo pipefail

SOFT=/opt/software
MOD=/opt/module
HOSTNAME_NEW=master
MYSQL_ROOT_PWD=123456
PROJECT_CODE=/opt/project/gx_eshop        # 项目代码放置目录

JDK_DIR=$MOD/jdk1.8.0_202
HADOOP_DIR=$MOD/hadoop-3.3.4
HIVE_DIR=$MOD/apache-hive-3.1.2-bin
SPARK_DIR=$MOD/spark-3.3.0
KAFKA_DIR=$MOD/kafka
CONDA_DIR=$MOD/miniconda3

log()  { echo -e "\n\033[1;36m==== $* ====\033[0m"; }
ok()   { echo -e "\033[1;32m  ✔ $*\033[0m"; }
warn() { echo -e "\033[1;33m  ! $*\033[0m"; }

[ "$(id -u)" = "0" ] || { echo "请用 root 执行"; exit 1; }
mkdir -p "$SOFT" "$MOD"

# ------------------------------------------------------------
# 0. 系统基础配置
# ------------------------------------------------------------
log "0. 系统基础配置"

hostnamectl set-hostname $HOSTNAME_NEW
grep -q "$HOSTNAME_NEW" /etc/hosts || echo "127.0.0.1 $HOSTNAME_NEW" >> /etc/hosts

setenforce 0 2>/dev/null || true
sed -i 's/^SELINUX=enforcing/SELINUX=disabled/' /etc/selinux/config 2>/dev/null || true
systemctl stop firewalld 2>/dev/null || true
systemctl disable firewalld 2>/dev/null || true
ok "主机名/hosts/SELinux/防火墙 处理完成"

# 时间同步（失败不阻塞）
yum install -y ntpdate >/dev/null 2>&1 || true
ntpdate ntp.aliyun.com >/dev/null 2>&1 || warn "时间同步失败，忽略"

# ------------------------------------------------------------
# 1. 修复 yum 源（CentOS7 已 EOL，官方源已下线）
# ------------------------------------------------------------
log "1. 修复 yum 源"

if ! yum repolist >/dev/null 2>&1; then
  warn "默认 yum 源不可用，切换到阿里云 vault 镜像"
  mkdir -p /etc/yum.repos.d/bak && mv /etc/yum.repos.d/*.repo /etc/yum.repos.d/bak/ 2>/dev/null || true
  cat > /etc/yum.repos.d/CentOS-Vault.repo <<'EOF'
[base]
name=CentOS-7.9.2009 - Base
baseurl=https://mirrors.aliyun.com/centos-vault/7.9.2009/os/$basearch/
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7
enabled=1

[updates]
name=CentOS-7.9.2009 - Updates
baseurl=https://mirrors.aliyun.com/centos-vault/7.9.2009/updates/$basearch/
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7
enabled=1
EOF
  yum clean all >/dev/null 2>&1
  yum makecache >/dev/null 2>&1 || warn "makecache 失败，继续"
fi
yum install -y vim wget lsof net-tools unzip tar gzip bzip2 \
               libaio numactl-libs nc tree >/dev/null 2>&1 || warn "部分基础包未装上，继续"
ok "yum 源可用，基础依赖安装完成"

# ------------------------------------------------------------
# 2. JDK 1.8
# ------------------------------------------------------------
log "2. 安装 JDK 1.8"
if [ ! -d "$JDK_DIR" ]; then
  tar -zxf $SOFT/jdk-8u202-linux-x64.tar.gz -C $MOD
  ok "JDK 解压完成"
else
  warn "JDK 已存在，跳过"
fi

# 统一环境变量
cat > /etc/profile.d/bigdata.sh <<EOF
export JAVA_HOME=$JDK_DIR
export HADOOP_HOME=$HADOOP_DIR
export HIVE_HOME=$HIVE_DIR
export SPARK_HOME=$SPARK_DIR
export KAFKA_HOME=$KAFKA_DIR
export CONDA_HOME=$CONDA_DIR
export PATH=\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$HIVE_HOME/bin:\$SPARK_HOME/bin:\$KAFKA_HOME/bin:\$CONDA_HOME/bin:\$PATH
EOF
source /etc/profile.d/bigdata.sh
ok "环境变量写入 /etc/profile.d/bigdata.sh"

# ------------------------------------------------------------
# 3. Hadoop 3.3.4
# ------------------------------------------------------------
log "3. 安装配置 Hadoop"
if [ ! -d "$HADOOP_DIR" ]; then
  tar -zxf $SOFT/hadoop-3.3.4.tar.gz -C $MOD
fi
source /etc/profile.d/bigdata.sh

# java 路径（Hadoop 用软链指向 JDK）
if [ ! -e "$HADOOP_DIR/libexec/java" ] && [ -d "$HADOOP_DIR/libexec" ]; then
  ln -sfn "$JDK_DIR" "$HADOOP_DIR/libexec/java" 2>/dev/null || true
fi

# hadoop-env.sh
cat >> $HADOOP_DIR/etc/hadoop/hadoop-env.sh <<EOF

# ==== 环境相关变量 ====
export JAVA_HOME=$JDK_DIR
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root
EOF

# core-site.xml
cat > $HADOOP_DIR/etc/hadoop/core-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://master:8020</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/opt/module/hadoop-3.3.4/data/tmp</value>
  </property>
  <property>
    <name>hadoop.http.staticuser.user</name>
    <value>root</value>
  </property>
</configuration>
EOF

# hdfs-site.xml
cat > $HADOOP_DIR/etc/hadoop/hdfs-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///opt/module/hadoop-3.3.4/data/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///opt/module/hadoop-3.3.4/data/datanode</value>
  </property>
  <property>
    <name>dfs.namenode.secondary.http-address</name>
    <value>master:9868</value>
  </property>
  <property>
    <name>dfs.webhdfs.enabled</name>
    <value>true</value>
  </property>
</configuration>
EOF

# mapred-site.xml
cat > $HADOOP_DIR/etc/hadoop/mapred-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*</value>
  </property>
</configuration>
EOF

# yarn-site.xml
cat > $HADOOP_DIR/etc/hadoop/yarn-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>master</value>
  </property>
  <property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
  </property>
  <!-- 虚拟机内存有限，放宽内存/虚拟内存检查 -->
  <property>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>
</configuration>
EOF

echo "master" > $HADOOP_DIR/etc/hadoop/workers
ok "Hadoop 配置完成"

# 格式化 NameNode（只做一次）
if [ ! -d "$HADOOP_DIR/data/namenode" ]; then
  log "3.1 格式化 HDFS NameNode"
  $HADOOP_DIR/bin/hdfs namenode -format -force >/dev/null 2>&1 || true
  ok "NameNode 格式化完成"
fi

# ------------------------------------------------------------
# 4. MySQL 8（元数据库 + 业务库）
# ------------------------------------------------------------
log "4. 安装配置 MySQL"
if ! command -v mysqld >/dev/null 2>&1; then
  if [ -f "$SOFT/mysql-8.0.46-1.el7.x86_64.rpm-bundle.tar" ]; then
    mkdir -p $SOFT/mysql_rpm && tar -xf $SOFT/mysql-8.0.46-1.el7.x86_64.rpm-bundle.tar -C $SOFT/mysql_rpm
    yum localinstall -y $SOFT/mysql_rpm/mysql-community-{common,libs,icu-data-files,client,server}-*.rpm >/dev/null 2>&1 \
      || yum localinstall -y $SOFT/mysql_rpm/mysql-community-*.rpm >/dev/null 2>&1
    systemctl enable mysqld >/dev/null 2>&1 || true
    systemctl start mysqld
    # 取临时密码
    TMP_PWD=$(grep 'temporary password' /var/log/mysqld.log | tail -1 | awk '{print $NF}')
    if [ -n "${TMP_PWD:-}" ]; then
      # MySQL 8 默认密码策略要求“大小写+数字+符号”，直接设 123456 会被拒绝（ERROR 1819）。
      # 所以先设一个合规密码，再降策略、改回 123456。
      mysql -uroot -p"$TMP_PWD" --connect-expired-password -e \
        "ALTER USER 'root'@'localhost' IDENTIFIED BY 'Root@123456';" 2>/dev/null || true
      mysql -uroot -p'Root@123456' -e \
        "SET GLOBAL validate_password.policy=LOW; SET GLOBAL validate_password.length=6;" 2>/dev/null || true
      mysql -uroot -p'Root@123456' -e \
        "ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '$MYSQL_ROOT_PWD';" 2>/dev/null || true
    else
      warn "未取到 MySQL 临时密码，若为全新初始化请手动执行："
      warn "  grep 'temporary password' /var/log/mysqld.log"
    fi
  else
    warn "未找到 MySQL rpm bundle，尝试 yum 安装 mariadb（仅兜底）"
    yum install -y mariadb-server mariadb >/dev/null 2>&1 || true
    systemctl enable mariadb >/dev/null 2>&1 || true
    systemctl start mariadb || true
  fi
else
  systemctl start mysqld 2>/dev/null || true
  warn "MySQL 已安装，跳过安装步骤"
fi

# 建库建表
cat > /tmp/mysql_init.sql <<'EOF'
CREATE DATABASE IF NOT EXISTS gx_ec_mysql DEFAULT CHARACTER SET utf8mb4;
CREATE DATABASE IF NOT EXISTS metastore DEFAULT CHARACTER SET latin1;
CREATE USER IF NOT EXISTS 'hive'@'%' IDENTIFIED BY 'hive123456';
GRANT ALL PRIVILEGES ON metastore.* TO 'hive'@'%';
GRANT ALL PRIVILEGES ON gx_ec_mysql.* TO 'hive'@'%';
-- MySQL 8 已不支持 GRANT ... IDENTIFIED BY，必须分开写
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED WITH mysql_native_password BY '123456';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
EOF
mysql -uroot -p$MYSQL_ROOT_PWD < /tmp/mysql_init.sql 2>/dev/null && ok "MySQL 库/授权创建完成" \
  || warn "MySQL 初始化 SQL 执行失败，请检查 root 密码（脚本默认 $MYSQL_ROOT_PWD）"

# ------------------------------------------------------------
# 5. Hive 3.1.2
# ------------------------------------------------------------
log "5. 安装配置 Hive"
if [ ! -d "$HIVE_DIR" ]; then
  tar -zxf $SOFT/apache-hive-3.1.2-bin.tar.gz -C $MOD
fi

# 5.1 MySQL 驱动
cp -f $SOFT/mysql-connector-j-8.0.33.jar $HIVE_DIR/lib/ 2>/dev/null || \
  cp -f /usr/share/java/mysql-connector-java.jar $HIVE_DIR/lib/ 2>/dev/null || \
  warn "未找到 MySQL 驱动 jar，请手动放到 $HIVE_DIR/lib/"

# 5.2 解决 Hive 与 Hadoop 的 Guava 版本冲突（经典报错：NoSuchMethodError Preconditions.checkArgument）
if [ -f "$HADOOP_DIR/share/hadoop/common/lib/guava-27.0-jre.jar" ]; then
  rm -f $HIVE_DIR/lib/guava-19.0.jar
  cp -f $HADOOP_DIR/share/hadoop/common/lib/guava-27.0-jre.jar $HIVE_DIR/lib/
  ok "Guava 冲突已修复（19.0 → 27.0）"
fi

# 5.3 hive-env.sh
cat > $HIVE_DIR/conf/hive-env.sh <<EOF
export JAVA_HOME=$JDK_DIR
export HADOOP_HOME=$HADOOP_DIR
export HIVE_CONF_DIR=$HIVE_DIR/conf
export HIVE_AUX_JARS_PATH=$HIVE_DIR/lib
EOF

# 5.4 hive-site.xml —— 元数据库走 MySQL，并开启 thrift metastore（Spark 也连它）
cat > $HIVE_DIR/conf/hive-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://master:3306/metastore?createDatabaseIfNotExist=true&amp;useSSL=false&amp;allowPublicKeyRetrieval=true&amp;serverTimezone=Asia/Shanghai</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>com.mysql.cj.jdbc.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>root</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>123456</value>
  </property>

  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
  </property>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://master:9083</value>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateAll</name>
    <value>true</value>
  </property>

  <property>
    <name>hive.server2.thrift.port</name>
    <value>10000</value>
  </property>
  <property>
    <name>hive.server2.thrift.bind.host</name>
    <value>master</value>
  </property>
  <property>
    <name>hive.server2.enable.doAs</name>
    <value>false</value>
  </property>

  <!-- 关闭 Hive 3 的元数据校验，避免与 Spark 内置 Hive 客户端版本冲突 -->
  <property>
    <name>hive.metastore.event.db.notification.api.auth</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.strict.managed.tables</name>
    <value>false</value>
  </property>
</configuration>
EOF

# 5.5 初始化元数据库（幂等：已初始化会报错但不影响）
if ! mysql -uroot -p$MYSQL_ROOT_PWD -e "use metastore; select 1 from VERSION limit 1" >/dev/null 2>&1; then
  log "5.1 初始化 Hive 元数据库（schematool）"
  schematool -dbType mysql -initSchema >/tmp/schematool.log 2>&1 && ok "元数据库初始化成功" \
    || { warn "schematool 失败，日志："; tail -20 /tmp/schematool.log; }
else
  warn "Hive 元数据库已初始化，跳过"
fi

# ------------------------------------------------------------
# 6. Spark 3.3.0
# ------------------------------------------------------------
log "6. 安装配置 Spark"
if [ ! -d "$SPARK_DIR" ]; then
  tar -zxf $SOFT/spark-3.3.0-bin-hadoop3.tgz -C $MOD
  # 压缩包解出来是 spark-3.3.0-bin-hadoop3，统一改成 spark-3.3.0
  if [ -d "$MOD/spark-3.3.0-bin-hadoop3" ]; then
    mv "$MOD/spark-3.3.0-bin-hadoop3" "$SPARK_DIR"
  fi
fi

cat > $SPARK_DIR/conf/spark-env.sh <<EOF
export JAVA_HOME=$JDK_DIR
export HADOOP_CONF_DIR=$HADOOP_DIR/etc/hadoop
export YARN_CONF_DIR=$HADOOP_DIR/etc/hadoop
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_MASTER_HOST=master
EOF

# 6.1 ★ 关键：把 Kafka 连接器 4 个 jar 放进 Spark jars 目录
for j in spark-sql-kafka-0-10_2.12-3.3.0.jar \
         spark-token-provider-kafka-0-10_2.12-3.3.0.jar \
         kafka-clients-3.2.1.jar \
         commons-pool2-2.11.0.jar; do
  if [ -f "$SOFT/$j" ]; then
    cp -f "$SOFT/$j" $SPARK_DIR/jars/
    ok "已放入 $j"
  else
    warn "缺少 $j（缺了会报 Failed to find data source: kafka）"
  fi
done

# 6.2 Spark 访问 Hive 元数据
cp -f $HIVE_DIR/conf/hive-site.xml $SPARK_DIR/conf/
cat > $SPARK_DIR/conf/spark-defaults.conf <<EOF
spark.master                     local[2]
spark.sql.warehouse.dir          hdfs://master:8020/user/hive/warehouse
spark.sql.hive.metastore.version 3.1.2
spark.sql.hive.metastore.jars    path
spark.sql.hive.metastore.jars.path file://$HIVE_DIR/lib/*
spark.sql.shuffle.partitions     4
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.driver.memory              1g
spark.eventLog.enabled           false
EOF
ok "Spark 配置完成（含 Kafka 连接器与 Hive 元数据对接）"

# ------------------------------------------------------------
# 7. Kafka 3.2.1（含 Zookeeper）
# ------------------------------------------------------------
log "7. 安装配置 Kafka"
if [ ! -d "$KAFKA_DIR" ]; then
  tar -zxf $SOFT/kafka_2.12-3.2.1.tgz -C $MOD
  mv $MOD/kafka_2.12-3.2.1 $KAFKA_DIR
fi
mkdir -p $KAFKA_DIR/data $KAFKA_DIR/zkdata

cat > $KAFKA_DIR/config/server.properties <<'EOF'
broker.id=0
listeners=PLAINTEXT://master:9092
advertised.listeners=PLAINTEXT://master:9092
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/opt/module/kafka/data
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=master:2181
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
EOF

cat > $KAFKA_DIR/config/zookeeper.properties <<'EOF'
dataDir=/opt/module/kafka/zkdata
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
EOF
ok "Kafka 配置完成"

# ------------------------------------------------------------
# 8. Python 3.9（Miniconda）
# ------------------------------------------------------------
log "8. 安装 Python 3.9 环境"
if [ ! -d "$CONDA_DIR" ]; then
  CONDA_SH=$(ls $SOFT/Miniconda3-py39_*.sh 2>/dev/null | head -1)
  if [ -n "${CONDA_SH:-}" ]; then
    bash "$CONDA_SH" -b -p $CONDA_DIR >/dev/null 2>&1
    ok "Miniconda 安装完成"
  else
    warn "未找到 Miniconda 安装包，跳过（可手动装 python3.9）"
  fi
fi

if [ -x "$CONDA_DIR/bin/pip" ]; then
  # 注意版本锁定：pandas 2.1+ / numpy 2.x 在 py3.9 上没有预编译 wheel，
  # 会退化成源码编译；而 Minimal 系统没有 gcc，必然失败
  $CONDA_DIR/bin/pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple \
      "numpy==1.24.4" "pandas==2.0.3" "scikit-learn==1.3.2" "joblib==1.3.2" 2>&1 | tail -2
  $CONDA_DIR/bin/pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple \
      kafka-python flask flask-cors pymysql 2>&1 | tail -2
  ok "Python 依赖安装完成（kafka-python/flask/pymysql/pandas/sklearn）"
fi

# ------------------------------------------------------------
# 9. 启动顺序：ZK → Kafka → Hadoop → Hive Metastore
# ------------------------------------------------------------
log "9. 启动组件并建库建表"

# Hadoop 的 start-dfs.sh 会 ssh 到本机，必须先配好免密
if [ ! -f /root/.ssh/id_rsa ]; then
  ssh-keygen -t rsa -N "" -f /root/.ssh/id_rsa >/dev/null 2>&1
fi
mkdir -p /root/.ssh && chmod 700 /root/.ssh
grep -q "$(cat /root/.ssh/id_rsa.pub)" /root/.ssh/authorized_keys 2>/dev/null || \
  cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys
systemctl start sshd 2>/dev/null || true
ok "root 免密 SSH 配置完成"

$KAFKA_DIR/bin/zookeeper-server-start.sh -daemon $KAFKA_DIR/config/zookeeper.properties
sleep 5
$KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties
sleep 8

$HADOOP_DIR/sbin/start-dfs.sh >/dev/null 2>&1 || warn "HDFS 启动异常，稍后手动检查"
$HADOOP_DIR/sbin/start-yarn.sh >/dev/null 2>&1 || warn "YARN 启动异常，稍后手动检查"
sleep 5

# Hive metastore 常驻（Spark 也要连它）
nohup $HIVE_DIR/bin/hive --service metastore >/tmp/metastore.log 2>&1 &
sleep 10

# Kafka Topic
$KAFKA_DIR/bin/kafka-topics.sh --create --if-not-exists \
  --topic guangxi-eshop-stream --bootstrap-server master:9092 \
  --partitions 1 --replication-factor 1 2>/dev/null && ok "Topic guangxi-eshop-stream 已创建" \
  || warn "Topic 创建失败（可能已存在）"

# Hive 建库建表
if [ -f "$PROJECT_CODE/sql/hive_ddl.sql" ]; then
  $HIVE_DIR/bin/hive -f "$PROJECT_CODE/sql/hive_ddl.sql" >/tmp/hive_ddl.log 2>&1 \
    && ok "Hive 四层表创建完成" || warn "Hive 建表失败，见 /tmp/hive_ddl.log"
else
  warn "未找到 $PROJECT_CODE/sql/hive_ddl.sql，跳过建表"
fi

# MySQL 业务表
if [ -f "$PROJECT_CODE/sql/mysql_ddl.sql" ]; then
  mysql -uroot -p$MYSQL_ROOT_PWD < "$PROJECT_CODE/sql/mysql_ddl.sql" 2>/dev/null \
    && ok "MySQL 业务表创建完成" || warn "MySQL 建表失败"
fi

# ------------------------------------------------------------
# 10. 验收
# ------------------------------------------------------------
log "10. 环境验收"
echo "--- 进程 ---"
jps | grep -E "NameNode|DataNode|ResourceManager|NodeManager" || true
echo "--- HDFS ---"
$HADOOP_DIR/bin/hdfs dfs -ls / 2>/dev/null | head -5 || warn "HDFS 不可用"
echo "--- Kafka Topic ---"
$KAFKA_DIR/bin/kafka-topics.sh --list --bootstrap-server master:9092 2>/dev/null || warn "Kafka 不可用"
echo "--- Hive ---"
$HIVE_DIR/bin/hive -e "show databases;" 2>/dev/null || warn "Hive 不可用（metastore 未起？）"
echo "--- MySQL ---"
mysql -uroot -p$MYSQL_ROOT_PWD -e "show databases;" 2>/dev/null || warn "MySQL 不可用"

cat <<'EOF'

============================================================
  安装完成。Web 入口：
    HDFS   http://master的IP:9870
    YARN   http://master的IP:8088
    Kafka  master:9092（Topic: guangxi-eshop-stream）
    MySQL  master:3306（root/123456，业务库 gx_ec_mysql）
  Hive 库：gx_ec_hive     元数据库：metastore（端口 9083）
============================================================
下一步：把项目代码放到 /opt/project/gx_eshop，按 README 依次启动：
  1) 生产者   python kafka_producer.py --ip master
  2) 流任务   spark-submit --master local[2] spark_streaming.py --mode stream
  3) 离线ETL  spark-submit --master local[2] offline_etl.py --dt all
  4) 预测     python ml_forecast.py
  5) 接口     python flask_server.py
  6) 大屏     浏览器打开 dashboard/dashboard.html
============================================================
EOF
