package com.learn.kafka.Config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.LoggingErrorHandler;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.backoff.FixedBackOff;

import com.learn.kafka.Services.LibraryEventService;

import ch.qos.logback.core.Context;
import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

	@Autowired
	private LibraryEventService libraryEventService;

	@Bean
	@ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
	ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ConsumerFactory<Object, Object> kafkaConsumerFactory) {
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
//		factory.setConcurrency(3);
//		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
		factory.setErrorHandler((thrownException, data) -> {
			log.error("Exception in consumerConfig is {} and the record is {}", thrownException.getMessage(), data);
		});
		factory.setRetryTemplate(retryTemplate());
		factory.setRecoveryCallback(context -> {
			if (context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
				log.info("Inside Recoverable logic");
				/*
				 * Arrays.asList(context.attributeNames()).forEach(attributeName -> {
				 * log.info("Attribute Name is : {}", attributeName);
				 * log.info("Attribute Value is : {}", context.getAttribute(attributeName)); });
				 */
				ConsumerRecord<Integer, String> consumerRecord = (ConsumerRecord<Integer, String>) context
						.getAttribute("record");
				libraryEventService.handleRecovery(consumerRecord);
			} else {
				log.info("Inside non-recoverable logic");
				throw new RuntimeException(context.getLastThrowable().getMessage());
			}
			return null;
		});
		return factory;
	}

	private RetryTemplate retryTemplate() {
		FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
		fixedBackOffPolicy.setBackOffPeriod(1000);
		RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setRetryPolicy(retryPolicy());
		retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
		return retryTemplate;
	}

	private RetryPolicy retryPolicy() {
//		SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
//		simpleRetryPolicy.setMaxAttempts(3);
		Map<Class<? extends Throwable>, Boolean> exceptionsMap = new HashMap<>();
		exceptionsMap.put(IllegalArgumentException.class, false);
		exceptionsMap.put(RecoverableDataAccessException.class, true);
		SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, exceptionsMap, true);
		return simpleRetryPolicy;
	}

}
