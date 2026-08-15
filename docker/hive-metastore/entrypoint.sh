#!/bin/bash
set -e

: "${METASTORE_DB_HOST:=metastore-db}"
: "${METASTORE_DB_PORT:=5432}"
: "${METASTORE_DB_NAME:=metastore}"
: "${METASTORE_DB_USER:=hive}"
: "${METASTORE_DB_PASS:=hive}"
: "${S3_ENDPOINT:=http://minio:9000}"
: "${S3_ACCESS_KEY:=minioadmin}"
: "${S3_SECRET_KEY:=minioadmin}"
: "${S3_WAREHOUSE:=s3a://lakehouse/warehouse/}"

JDBC_URL="jdbc:postgresql://${METASTORE_DB_HOST}:${METASTORE_DB_PORT}/${METASTORE_DB_NAME}"

cat > "${HIVE_HOME}/conf/hive-site.xml" <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <!-- JDBC backend -->
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>${JDBC_URL}?createDatabaseIfNotExist=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>${METASTORE_DB_USER}</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>${METASTORE_DB_PASS}</value>
  </property>

  <!-- Warehouse location on S3/MinIO -->
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>${S3_WAREHOUSE}</value>
  </property>

  <!-- S3 / MinIO filesystem -->
  <property>
    <name>fs.s3a.endpoint</name>
    <value>${S3_ENDPOINT}</value>
  </property>
  <property>
    <name>fs.s3a.access.key</name>
    <value>${S3_ACCESS_KEY}</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>${S3_SECRET_KEY}</value>
  </property>
  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
  <property>
    <name>fs.s3a.connection.ssl.enabled</name>
    <value>false</value>
  </property>

  <!-- Thrift metastore -->
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://0.0.0.0:9083</value>
  </property>
  <property>
    <name>hive.metastore.event.db.notification.api.auth</name>
    <value>false</value>
  </property>

  <!-- Schema verification -->
  <property>
    <name>datanucleus.schema.autoCreateAll</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>true</value>
  </property>
</configuration>
EOF

cat > "${HADOOP_HOME}/etc/hadoop/core-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.s3a.endpoint</name>
    <value>${S3_ENDPOINT}</value>
  </property>
  <property>
    <name>fs.s3a.access.key</name>
    <value>${S3_ACCESS_KEY}</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>${S3_SECRET_KEY}</value>
  </property>
  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
  <property>
    <name>fs.s3a.connection.ssl.enabled</name>
    <value>false</value>
  </property>
</configuration>
EOF

echo "Waiting for PostgreSQL at ${METASTORE_DB_HOST}:${METASTORE_DB_PORT}..."
until nc -z "${METASTORE_DB_HOST}" "${METASTORE_DB_PORT}" 2>/dev/null; do
    sleep 2
done
echo "PostgreSQL is ready."

echo "Initialising Hive Metastore schema..."
${HIVE_HOME}/bin/schematool -dbType postgres -initSchema --verbose 2>&1 || {
    echo "Schema already initialised (or upgrade needed). Attempting upgrade..."
    ${HIVE_HOME}/bin/schematool -dbType postgres -upgradeSchema --verbose 2>&1 || true
}

echo "Starting Hive Metastore on port 9083..."
exec ${HIVE_HOME}/bin/hive --service metastore
