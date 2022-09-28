package com.pl.jellytech.yavaconf.spring.kafka.consumer.service;

import com.pl.jellytech.yavaconf.spring.kafka.consumer.listeners.HeaderListener;
import com.pl.jellytech.yavaconf.spring.kafka.consumer.listeners.PartitionListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
@Slf4j
public class KafkaConsumerService {

	private final HeaderListener headerListener;
	private final PartitionListener partitionListener;
	private final KafkaTemplate<String, String> kafkaTemplate;


	@Autowired
	public KafkaConsumerService(HeaderListener headerListener, PartitionListener partitionListener, KafkaTemplate<String, String> kafkaTemplate) {
		this.headerListener = headerListener;
		this.partitionListener = partitionListener;
		this.kafkaTemplate = kafkaTemplate;
	}

	public void receiveMessagesFromTopic(String topic, Long timeToLive){
		long start = System.currentTimeMillis();
		ConsumerRecord<String, String> recordReceived = new ConsumerRecord<>(topic, 0, 0, "", "");
		while(recordReceived != null){
			log.info("Received message on topic {topic} and partition 0 at offset {}", recordReceived.offset());
			recordReceived = this.kafkaTemplate.receive(topic, 0, 0, Duration.ofMillis(timeToLive));
		}
		log.debug("Time passed: {} ms", System.currentTimeMillis() - start);
	}
}
