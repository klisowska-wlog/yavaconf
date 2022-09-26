package com.pl.jellytech.yavaconf.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaProducerManager {
	private final static Logger logger = LogManager.getLogger(KafkaProducerManager.class);

	private final static String BOOTSTRAP_SERVERS = "localhost:9092";

	private static Map<String, Object> getProperties(){
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		return props;
	}

	public static KafkaProducer<String, String> createProducer(){
		logger.debug("Creating new producer");
		return new KafkaProducer<>(getProperties());
	}

	public void continuouslyProduceMessages(KafkaProducer<String, String> producer, List<String> topics){
		int counter = 0;
		while(true){
			String message = "Continuous message " + counter++;
			for (String topic : topics) {
				logger.info("Writing message {} to topic {}", message, topic);
				producer.send(new ProducerRecord<>(topic, null, message));
			}
			try {
				logger.debug("Waiting 1s before writing next message ...");
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error("Producer second sleep interrupted: {}.\nStopping producing messages...", e.getMessage(), e);
				producer.close();
				break;
			}
		}
	}
}
