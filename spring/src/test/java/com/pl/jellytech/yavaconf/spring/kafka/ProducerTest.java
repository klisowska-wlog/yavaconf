package com.pl.jellytech.yavaconf.spring.kafka;

import com.pl.jellytech.yavaconf.spring.kafka.producer.service.KafkaProducerService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static com.pl.jellytech.yavaconf.spring.kafka.producer.service.KafkaProducerService.ERROR_RESPONSE;
import static com.pl.jellytech.yavaconf.spring.kafka.producer.service.KafkaProducerService.SUCCESS_RESPONSE;

@SpringBootTest
public class ProducerTest {

    private final KafkaProducerService producerService;

    @Autowired
    public ProducerTest(KafkaProducerService producerService){
        this.producerService = producerService;
    }

    @Test
    @Disabled // should only be called manually
    public void startContinuousProducer(){
        String topic = "spring-continuous";

        producerService.continouslyProduceToTopic(topic);
    }

    @Test
    public void writeMessageAndAcknowledge(){
        String topic = "spring-topic";
        String message = "My spring topic for writeMessageAndAcknowledge test";

        String result = this.producerService.sendMessageAndWaitForResult(topic, message);

        Assertions.assertEquals(SUCCESS_RESPONSE, result, "Expected sending message to be a success");
    }

    @Test
    public void writeMessageAndAcknowledgeFailure(){
        String topic = ""; // invalid topic name
        String message = "My spring topic for FAILED writeMessageAndAcknowledge test";

        String result = this.producerService.sendMessageAndWaitForResult(topic, message);

        Assertions.assertEquals(ERROR_RESPONSE, result, "Empty topic should not be accepted!");
    }
}
