package com.thinkmicroservices.natsservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.nativex.hint.TypeHint;

/**
 * This class provides the service entrypoint for the NATS REST service.
 * @author cwoodward
 */

@ComponentScan(basePackages = "com.thinkmicroservices")
/* include the Spring Native @TypeHint to enable NATS SocketDataPort inclusion 
in executable image*/
@TypeHint(types = io.nats.client.impl.SocketDataPort.class, typeNames = "io.nats.client.impl.SocketDataPort")
@SpringBootApplication
public class NatsProcessorServiceApplication {

    /**
     * 
     * @param args 
     */
	public static void main(String[] args) {
		SpringApplication.run(NatsProcessorServiceApplication.class, args);
	}

}
