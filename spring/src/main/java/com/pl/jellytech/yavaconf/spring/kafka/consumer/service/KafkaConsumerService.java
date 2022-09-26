package com.pl.jellytech.yavaconf.spring.kafka.consumer.service;

import com.pl.jellytech.yavaconf.spring.kafka.consumer.listeners.HeaderListener;
import com.pl.jellytech.yavaconf.spring.kafka.consumer.listeners.PartitionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

	private final HeaderListener headerListener;
	private final PartitionListener partitionListener;

	@Autowired
	public KafkaConsumerService(HeaderListener headerListener, PartitionListener partitionListener) {
		this.headerListener = headerListener;
		this.partitionListener = partitionListener;
	}


}
