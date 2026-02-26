FROM eclipse-temurin:17-jre-jammy

ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3

RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-pip \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
  | tar -xz -C /opt

RUN mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV AWS_REGION=eu-north-1

# S3 support
RUN curl -L -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar \
 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -L -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar \
 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY docker/spark/hive-site.xml /opt/spark/conf/hive-site.xml
COPY requirements /tmp/requirements
RUN mkdir -p /opt/spark/warehouse && chmod -R 777 /opt/spark/warehouse

# Delta Lake jars
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/delta-spark.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -L -o /opt/spark/jars/delta-storage.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar

RUN pip install --no-cache-dir -r /tmp/requirements/spark.txt
