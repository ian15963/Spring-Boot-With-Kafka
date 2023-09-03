package com.example.consumer.listener;

import com.example.consumer.custom.PersonCustomListener;
import com.example.consumer.model.City;
import com.example.consumer.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class TestListener {

    @KafkaListener(topics = "topic-1", groupId = "group-1")
    public void listen(List<String> messages,
                       //@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       //@Header(KafkaHeaders.RECEIVED_PARTITION) int partition
                       ConsumerRecordMetadata metadata){
        //log.info("Thread: {}", Thread.currentThread().getId());
        log.info(messages.toString());
        log.info("Topic: {}, Partition Id: {}, timestamp: {}", metadata.topic(), metadata.partition(), metadata.timestamp());
    }

    //Escutando partições específicas
    @KafkaListener(topicPartitions = {@TopicPartition(topic = "my-topic", partitions = "0")}, groupId = "my-group")
    public void listen2(String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition){
        log.info("Partition 0: {}, Message: {}", partition, message);
    }

    //Escutando partições específicas
    @KafkaListener(topicPartitions = {@TopicPartition(topic = "my-topic", partitions = "1-9")}, groupId = "my-group")
    public void listen3(String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition){
        log.info("Partition 1-9: {}, Message: {}", partition, message);
    }

    //@KafkaListener(topics = "topic-person", groupId = "group-1", containerFactory = "personKafkaListenerContainerFactory")
   @PersonCustomListener(groupId = "group-1")
    public void listen(Person person){
        log.info("Pessoa: {}", person);
    }

    @KafkaListener(topics = "city-topic", groupId = "group-1", containerFactory = "jsonConcurrentKafkaListenerContainerFactory")
    public void listen(List<City> cities){
        log.info("Cidades: {}", cities);
    }
}
