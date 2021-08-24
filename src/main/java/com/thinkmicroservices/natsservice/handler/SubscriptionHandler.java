package com.thinkmicroservices.natsservice.handler;

import io.nats.client.Connection.Status;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.io.IOException;
import javax.annotation.PostConstruct;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Message;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * This class create a subscription to a NATS request/reply subject,
 * and then re-publishes it to be processed by a queue worker.
 * @author cwoodward
 */

@Slf4j
@Component
public class SubscriptionHandler implements ConnectionListener {

    @Value("${nats.servers}")
    private String[] servers;

    private Connection connection;
   
    private Dispatcher requestReplyDispatcher;

    private static final String NATS_PROTOCOL_STRING = "nats://";
   
    private static final String REQUEST_REPLY_SUBJECT = "nats.request-reply";
    private static final String QUEUE_SUBJECT = "nats.queue";
    private static final String RESPONSE_TEMPLATE_STRING = "<%s> processed by %s";

    /**
     * 
     * @return
     * @throws IOException
     * @throws InterruptedException 
     */
    private Connection getConnection() throws IOException, InterruptedException {
        
        if ((connection == null) || (connection.getStatus() == Status.DISCONNECTED)) {
           
            Options.Builder connectionBuilder = new Options.Builder().connectionListener(this);

            /* iterate over the list of servers to build NATS connections */
            
            for (String server : servers) {
                String natsServer = NATS_PROTOCOL_STRING + server;
                log.info("adding nats server:" + natsServer);
                connectionBuilder.server(natsServer).maxReconnects(-1);
            }

            connection = Nats.connect(connectionBuilder.build());
        }
        
        log.info("return connection:" + connection);

        return connection;
    }

    /**
     * Listen for Connection Events
     * @param cnctn
     * @param event 
     */
    @Override
    
    public void connectionEvent(Connection cnctn, Events event) {
        log.info("Connection Event:" + event);

        switch (event) {

            case CONNECTED:
                log.info("CONNECTED!");
                break;
            case DISCONNECTED:
                try {
                    connection = null;
                    getConnection();
                } catch (Exception ex) {
                    log.error(ex.getMessage(), ex);

                }
                break;
            case RECONNECTED:
                log.info("RECONNECTED!");
                break;
            case RESUBSCRIBED:
                log.info("RESUBSCRIBED!");
                break;

        }

    }

    /**
     * create the request reply dispatcher
     * @throws IOException
     * @throws InterruptedException 
     */
    private void createRequestReplyDispatcher() throws IOException, InterruptedException {
        
        log.info("create Request-reply dispatcher");

        requestReplyDispatcher = getConnection().createDispatcher(msg -> {
        });

        /* subscribe to the REQUEST_REPLY subject
        */
        
        requestReplyDispatcher.subscribe(REQUEST_REPLY_SUBJECT, msg -> {
            try {
                
                String incomingMessage = new String(msg.getData());
                log.info("Received Request: " + incomingMessage);
                log.info("Message Reply To: " + msg.getReplyTo());
                
                /* publish the message to a subject that is handled by queue works */
                String queueWorkerResponse = getConnection().request(QUEUE_SUBJECT, msg.getData())
                        .thenApply(Message::getData)
                        .thenApply(String::new)
                        .join();
                log.info("queue worker response:" + queueWorkerResponse);
                String responseMessage = String.format(RESPONSE_TEMPLATE_STRING, incomingMessage, queueWorkerResponse);
                getConnection().publish(msg.getReplyTo(), responseMessage.getBytes());
                
            } catch (IOException ex) {
                log.error(ex.getMessage(), ex);
            } catch (InterruptedException ex) {
                log.error(ex.getMessage(), ex);
            }
        });
    }

    /**
     *
     */
    private void destroyDispatchers() {
       
        requestReplyDispatcher.unsubscribe(REQUEST_REPLY_SUBJECT);
        log.info("Unsubscribed:"+REQUEST_REPLY_SUBJECT);
    }

    /**
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @PostConstruct
    void postConstruct() throws IOException, InterruptedException {
        
        log.info("SubscriptionHandler postCreate");
        createRequestReplyDispatcher();

    }

    /**
     *
     */
    @PreDestroy
    public void preDestroy() {

        try {
            
            destroyDispatchers();
            connection.close();
            
        } catch (InterruptedException ex) {
            log.warn("unable to close connection.");
        }
    }

}
