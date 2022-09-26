package com.pl.jellytech.yavaconf.kafka;

import com.pl.jellytech.yavaconf.kafka.producer.KafkaProducerManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Scanner;

public class KafkaProducerMain {
	private static final Logger logger = LogManager.getLogger(KafkaProducerMain.class);
	private static final Scanner scanner = new Scanner(System.in);

	public static void main( String[] args ) {
		logger.info("Started Kafka JAVA Producer API example");

		logger.info("Starting Kafka producer. Please provide topic to write messages to:");
		String topic = scanner.nextLine();

		KafkaProducer<String, String> producer = KafkaProducerManager.createProducer();

		logger.info("Start writing messages to topic {}. Exit by providing the message 'exit'", topic);
		logger.info("If you want to provide a key for the message use a '|' as a separator. Example: key | This is my message");
		String message = scanner.nextLine();
		while(!message.equals("exit")){
			String key = null;
			if(message.contains("|")) {
				int index = message.indexOf("|");
				key = message.substring(0, index);
				message = message.substring(index + 1);
			}
			logger.debug("Writing message {} with key {}", message, key);
			producer.send(new ProducerRecord<>(topic, key, message));

			message = scanner.nextLine();
		}
		logger.info("Finished writing messages to topic {}. Exiting application ...", topic);
	}
}
