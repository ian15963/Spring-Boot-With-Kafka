package com.example.consumer.listener;

import com.example.consumer.custom.PersonCustomListener;
import com.example.consumer.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TestListener {

    @KafkaListener(topics = "topic-x", groupId = "group-1")
    public void listen(String message,
                       //@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       //@Header(KafkaHeaders.RECEIVED_PARTITION) int partition
                       ConsumerRecordMetadata metadata){
        //log.info("Thread: {}", Thread.currentThread().getId());
        log.info(message);
        log.info("Topic: {}, Partition Id: {}, timestamp: {}", metadata.topic(), metadata.partition(), metadata.timestamp());
    }

    //@KafkaListener(topics = "topic-person", groupId = "group-1", containerFactory = "personKafkaListenerContainerFactory")
   @PersonCustomListener(groupId = "group-1")
    public void listen(Person person){
        log.info("Pessoa: {}", person);
    }
}
