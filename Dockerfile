FROM openjdk
VOLUME /pharbers_config
VOLUME /logs
COPY target/*.jar app.jar
COPY target/lib /lib
ENTRYPOINT exec java -jar ./app.jar