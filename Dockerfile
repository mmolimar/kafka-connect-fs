# This is the base docker image we use at spothero for kafka connect
FROM spothero/kafka-connect:5.4.1_aiven-jdbc-6.1.0_debezium-1.0.0.Final_mirrortool-3.1.0_jmx-prometheus-0.12.0

ARG PROJECT_VERSION

COPY ./target/kafka-connect-fs-${PROJECT_VERSION}-package /usr/share/java/kafka-connect-fs
