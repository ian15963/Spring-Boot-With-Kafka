package com.example.consumer.config;

import com.example.consumer.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;

@Slf4j
@EnableKafka
@Configuration
public class ConsumerKafkaConfig {

    private KafkaProperties kafkaProperties;

    public ConsumerKafkaConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory(){
        var configs = new HashMap<String, Object>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(configs);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainerFactory(){
        var factory =  new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setBatchListener(true);
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(2);
        return factory;
    }
// Desserialização de um objeto específico.
    @Bean
    public ConsumerFactory<String, Person> personConsumerFactory(){
        var configs = new HashMap<String, Object>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        var jsonDeserializer = new JsonDeserializer<>(Person.class).trustedPackages("*").forKeys();
        return new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), jsonDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Person> personKafkaListenerContainerFactory(){
        var factory =  new ConcurrentKafkaListenerContainerFactory<String, Person>();
        factory.setCommonErrorHandler(defaultErrorHandler());
        factory.setConsumerFactory(personConsumerFactory());
        factory.setRecordInterceptor(exampleInterceptor());
//        factory.setRecordInterceptor(adultInterceptor());
        return factory;
    }

    public ProducerFactory<String, Person> pf(){
        var configs = new HashMap<String, Object>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new JsonSerializer<>());
    }

    private CommonErrorHandler defaultErrorHandler() {
        var recoverer = new DeadLetterPublishingRecoverer(new KafkaTemplate<>(pf()));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000, 2));
    }

    private RecordInterceptor<String, Person> exampleInterceptor() {
        return new RecordInterceptor<String, Person>() {
            @Override
            public ConsumerRecord<String, Person> intercept(ConsumerRecord<String, Person> consumerRecord, Consumer<String, Person> consumer) {
                return consumerRecord;
            }

            @Override
            public void success(ConsumerRecord<String, Person> record, Consumer<String, Person> consumer) {
                log.info("Sucesso");
            }

            @Override
            public void failure(ConsumerRecord<String, Person> record, Exception exception, Consumer<String, Person> consumer) {
                log.info("Falha");
            }
        };
    }

    //Interceptor decide se vai consumir a informação de uma partição de um determinado tópico.
    private RecordInterceptor<String, Person> adultInterceptor(){
        return (consumerRecord, consumer) -> {
            log.info("consumerRecord: {}", consumerRecord);
            var person = consumerRecord.value();
            return person.getAge() >= 18 ? consumerRecord : null;
        };
    }

    @Bean
    public ConsumerFactory jsonConsumerFactory(){
        var configs = new HashMap<String, Object>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(configs);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory jsonConcurrentKafkaListenerContainerFactory(){
        var factory =  new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(jsonConsumerFactory());
        factory.setMessageConverter(new BatchMessagingMessageConverter(new JsonMessageConverter()));
        factory.setBatchListener(true);
//        factory.setMessageConverter(new JsonMessageConverter());
        return factory;
    }

}
