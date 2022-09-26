package com.pl.jellytech.yavaconf.spring.kafka.consumer.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class PartitionListener {
	private static final Logger logger = LoggerFactory.getLogger(PartitionListener.class);

	@KafkaListener(topicPartitions = @TopicPartition(topic = "${kafka.listener.partition.topic}",
			partitionOffsets = {@PartitionOffset(partition = "0", initialOffset = "0")}),
			containerFactory = "kafkaListenerContainerFactory",
			groupId = "partition")
	public void listenToFirstPartition(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		logger.info("Received Message {} from partition {}.", message, partition);
	}
}
