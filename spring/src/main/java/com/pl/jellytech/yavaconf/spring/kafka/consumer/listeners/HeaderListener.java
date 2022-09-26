package com.pl.jellytech.yavaconf.spring.kafka.consumer.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class HeaderListener {
	private static final Logger logger = LoggerFactory.getLogger(HeaderListener.class);

	@KafkaListener(topics = "${kafka.listener.header.topics}", groupId = "header")
	public void listenWithHeaders(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		logger.info("Received Message: {} from partition: {}", message, partition);
	}
}
