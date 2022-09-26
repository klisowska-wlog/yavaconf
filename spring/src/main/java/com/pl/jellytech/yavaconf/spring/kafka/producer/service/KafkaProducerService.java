package com.pl.jellytech.yavaconf.spring.kafka.producer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class KafkaProducerService {
	public static final String SUCCESS_RESPONSE = "SUCCESS";
	public static final String ERROR_RESPONSE = "ERROR";

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ExecutorService executor = Executors.newSingleThreadExecutor();

	@Autowired
	public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	public void sendMessage(String topic, String msg) {
		this.kafkaTemplate.send(topic, msg);
	}

	public void continouslyProduceToTopic(String topic) {
		int counter = 0;
		try {
			while(true){
				String message = "Continous message nr " + counter++;
				log.debug("Sending message {message}");
				this.kafkaTemplate.send(topic, message);
				TimeUnit.MILLISECONDS.sleep(1000);
			}
		} catch (InterruptedException e) {
			log.warn("Exited with exception {}", e.getMessage());
		}
	}

	private final ListenableFutureCallback<SendResult<String, String>> callback = new ListenableFutureCallback<>() {
		final long start = System.currentTimeMillis();
		@Override
		public void onSuccess(SendResult<String, String> result) {
			log.debug("Record received: {} (key) - {} (value)", result.getProducerRecord().key(), result.getProducerRecord().value());
			log.info("Sent message=[{}}] with offset=[{}}]. Time: {} ms", result.getProducerRecord().value(), result.getRecordMetadata().offset(), System.currentTimeMillis() - start);
		}

		@Override
		public void onFailure(Throwable ex) {
			log.error("Sending failed. Error: {}. Time: {} ms", ex.getMessage(), System.currentTimeMillis() - start);
		}
	};
	public String sendMessageAndWaitForResult(String topic, String message) {
		try {
			ListenableFuture<SendResult<String, String>> future = this.kafkaTemplate.send(topic, message);
			future.addCallback(callback);

			var result = future.get();
			if (result != null && result.getProducerRecord() != null){
				return "SUCCESS";
			}else{
				return "ERROR";
			}
		}catch (Exception ex){
			log.error("Exception occurred: {}", ex.getMessage(), ex);
			return "ERROR";
		}
	}
}
