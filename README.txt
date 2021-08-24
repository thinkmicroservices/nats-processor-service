The service requires a local NATS instance running on port 4222.
The following docker command will spin up a working instance the
service can use

docker run -p 4222:4222 -p 8222:8222 -p 6222:6222   nats:latest


To build the application as a jar with included dependencies:

mvn clean package

To run the jar application:

java -jar ./target/nats-processor-service-0.0.1-SNAPSHOT.jar 

To build the Spring Native GraalVM native executable

mvn clean spring-boot:build-image

