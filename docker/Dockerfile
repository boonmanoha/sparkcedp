FROM us.icr.io/ddp-common/spark:2.4.4
RUN set -ex && apt-get update && apt-get upgrade -y && rm -rf /var/cache/apt/*


COPY target /opt/spark/target
COPY lib/ibm-truststore.jks /opt/spark/project
COPY lib/ibm-truststore.jks /opt/
COPY lib/ibm-truststore.jks /project
COPY lib/mongo-dst.txt /opt/
COPY lib/mongo-dst.pem /opt/
COPY lib/mongo-dst.jks /opt/

ENV SPARK_HOME /opt/spark
WORKDIR /opt/spark/work-dir
ENTRYPOINT [ "/opt/entrypoint.sh" ]

