FROM confluentinc/cp-kafka-connect-base:5.5.1

ARG PROJECT_VERSION
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

COPY ./target/components/packages/mmolimar-kafka-connect-fs-${PROJECT_VERSION}.zip /tmp/kafka-connect-fs.zip
RUN confluent-hub install --no-prompt /tmp/kafka-connect-fs.zip && rm -rf /tmp/kafka-connect-fs.zip
