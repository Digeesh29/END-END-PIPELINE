FROM apache/airflow:2.3.1

USER root

# PySpark needs a real JVM to launch - install a lightweight headless JDK
# procps provides the 'ps' command Spark's launch scripts expect
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk-headless procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Download the JARs that let PySpark talk to S3-compatible storage (MinIO) via s3a://
# Versions matched to pyspark 3.4.4's bundled Hadoop 3.3.x
RUN mkdir -p /opt/spark_jars && \
    curl -L -o /opt/spark_jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark_jars/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

USER airflow

RUN pip install --no-cache-dir pyspark==3.4.4