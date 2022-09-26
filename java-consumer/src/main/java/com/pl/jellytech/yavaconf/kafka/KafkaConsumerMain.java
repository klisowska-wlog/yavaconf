package com.pl.jellytech.yavaconf.kafka;

import com.pl.jellytech.yavaconf.kafka.consumer.KafkaConsumerManager;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class KafkaConsumerMain {
	private static final Logger logger = LogManager.getLogger(KafkaConsumerMain.class);
	private static final Scanner scanner = new Scanner(System.in);

	public static void main( String[] args ) {
		logger.info("Started Kafka JAVA Consumer API example");

		logger.info("Starting Kafka consumer. Please provide topics to subscribe to separated by ',':");
		String topicsStr = scanner.nextLine();

		logger.info("Please provide group id to assign consumer to:");
		String groupId = scanner.nextLine();

		logger.info("Do you want to read from the beginning? (y / n)");
		boolean isEarliest = scanner.nextLine().equals("y");
		KafkaConsumer<String, String> consumer = KafkaConsumerManager.createConsumer(groupId, isEarliest);
		List<String> topics = Arrays.stream(topicsStr.split(",")).map(String::trim).collect(Collectors.toList());

		KafkaConsumerManager.infinitelyConsume(consumer, topics);

		logger.info("Finished reading messages from topics {}. Exiting application ...", topicsStr);
	}
}
