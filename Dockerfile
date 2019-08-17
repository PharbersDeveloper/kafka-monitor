FROM openjdk
VOLUME /pharbers_config
COPY pharbers_config pharbers_config
COPY target/*.jar app.jar
ENV PHA_CONF_HOME /pharbers_config
ENTRYPOINT exec java -jar ./app.jar