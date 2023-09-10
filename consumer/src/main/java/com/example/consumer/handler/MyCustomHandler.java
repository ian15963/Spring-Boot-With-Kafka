package com.example.consumer.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MyCustomHandler implements KafkaListenerErrorHandler {

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException e) {
        log.info("---Entrou no handler---");
        log.info("Payload: {}", message.getPayload());
        log.info("Exception: {}", e.toString());
//      Caso retorne null ou um objeto vai para o success do interceptor return null;
        throw e;
    }

}
