FROM eclipse-temurin:11-jre-focal

ARG HADOOP_VERSION=3.3.4
ARG HIVE_VERSION=3.1.3
ARG POSTGRES_JDBC_VERSION=42.7.3
ARG AWS_SDK_VERSION=1.12.262

ENV HADOOP_HOME=/opt/hadoop
ENV HIVE_HOME=/opt/hive
ENV PATH="${HIVE_HOME}/bin:${HADOOP_HOME}/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl procps netcat && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    | tar -xz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}

RUN curl -fsSL https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz \
    | tar -xz -C /opt && \
    mv /opt/apache-hive-${HIVE_VERSION}-bin ${HIVE_HOME}

# PostgreSQL JDBC
ADD --chmod=644 https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    ${HIVE_HOME}/lib/

# AWS / MinIO support
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar \
    ${HIVE_HOME}/lib/
ADD --chmod=644 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    ${HIVE_HOME}/lib/

# Remove Guava conflict (Hive ships old Guava; Hadoop needs newer)
RUN rm -f ${HIVE_HOME}/lib/guava-*.jar && \
    cp ${HADOOP_HOME}/share/hadoop/common/lib/guava-*.jar ${HIVE_HOME}/lib/

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 9083

ENTRYPOINT ["/entrypoint.sh"]
