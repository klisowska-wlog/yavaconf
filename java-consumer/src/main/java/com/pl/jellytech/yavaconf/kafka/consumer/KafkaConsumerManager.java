package com.pl.jellytech.yavaconf.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaConsumerManager {
	private final static Logger logger = LogManager.getLogger(KafkaConsumerManager.class);

	private final static String BOOTSTRAP_SERVERS = "localhost:9092";

	private static Map<String, Object> getProperties(){
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		return props;
	}

	public static KafkaConsumer<String, String> createConsumer(String groupId, boolean isEarliest){
		Map<String, Object> props = getProperties();
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, isEarliest ? "earliest" : "latest");

		logger.info("Starting new consumer for group {}", groupId);
		return new KafkaConsumer<>(props);
	}

	public static void infinitelyConsume(KafkaConsumer<String, String> consumer, List<String> topics){
		consumer.subscribe(topics);

		try (consumer) {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				records.forEach(record -> {
					logger.debug("Record retrieved: Key: {}, value: {}, partition: {}, offset: {})",
							record.key(), record.value(),
							record.partition(), record.offset());
				});
			}
		}
	}
}
