package com.example.producer.controller;

import com.example.producer.model.City;
import com.example.producer.model.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.stream.IntStream;

@RestController
@RequestMapping("/kafka/producer")
public class ProducerKafkaController {

//    private KafkaTemplate<String, String> kafkaTemplate;
//    private KafkaTemplate<String, Serializable> jsonKafkaTemplate;
      private RoutingKafkaTemplate kafkaTemplate;
    @Autowired
    public ProducerKafkaController(RoutingKafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/send")
    public void send(){
        IntStream.range(1, 10).boxed().forEach(n -> kafkaTemplate.send("topic-1", "Mensagem: " + n));
    }

//    @GetMapping("/send-2")
//    public void send2(){
//        kafkaTemplate.send("my-topic", "Teste");
//    }

    @GetMapping("/send-person")
    public void sendPerson(){
        Random random = new Random();
        kafkaTemplate.send("person-topic", new Person("Ian", random.nextInt(51)));
    }

    @GetMapping("/send-city")
    public void sendCity(){
        kafkaTemplate.send("city-topic", new City("Brejões", "BA"));
    }


}
