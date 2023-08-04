package com.example.producer.controller;

import com.example.producer.model.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.stream.IntStream;

@RestController
@RequestMapping("/kafka/producer")
public class ProducerKafkaController {

    private KafkaTemplate<String, String> kafkaTemplate;
    private KafkaTemplate<String, Serializable> jsonKafkaTemplate;

    @Autowired
    public ProducerKafkaController(KafkaTemplate<String, String> kafkaTemplate, KafkaTemplate<String, Serializable> jsonKafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.jsonKafkaTemplate = jsonKafkaTemplate;
    }

    @GetMapping("/send")
    public void send(){
        IntStream.range(1, 10).boxed().forEach(n -> kafkaTemplate.send("topic-1", "Mensagem: " + n));
    }

    @GetMapping("/send-2")
    public void send2(){
        kafkaTemplate.send("my-topic", "Teste");
    }

    @GetMapping("/send-person")
    public void sendPerson(){
        jsonKafkaTemplate.send("topic-person", new Person("Ian", 15));
    }


}
