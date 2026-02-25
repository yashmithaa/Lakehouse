FROM apache/spark:3.5.1

USER root

ENV PATH="/opt/spark/bin:${PATH}"

# ── Hudi + Kafka + AWS/S3 JARs ───────────────────────────────────────────────
# These versions are pinned to Spark 3.5 / Scala 2.12 compatibility.
ENV HUDI_VERSION=0.15.0
ENV SPARK_VERSION=3.5
ENV SCALA_VERSION=2.12
ENV AWS_SDK_VERSION=1.12.262
ENV HADOOP_AWS_VERSION=3.3.4

RUN mkdir -p /opt/spark/jars

# Hudi Spark bundle (includes Hudi core, Spark datasource, utilities)
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark${SPARK_VERSION}-bundle_${SCALA_VERSION}/${HUDI_VERSION}/hudi-spark${SPARK_VERSION}-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar \
    /opt/spark/jars/

# Spark–Kafka connector
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/3.5.1/spark-sql-kafka-0-10_${SCALA_VERSION}-3.5.1.jar \
    /opt/spark/jars/

# Kafka clients (needed at runtime by the connector)
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar \
    /opt/spark/jars/

# Spark token provider for Kafka
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/3.5.1/spark-token-provider-kafka-0-10_${SCALA_VERSION}-3.5.1.jar \
    /opt/spark/jars/

# Commons pool2 (dependency of kafka connector)
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
    /opt/spark/jars/

# AWS / S3 support (for MinIO)
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    /opt/spark/jars/
