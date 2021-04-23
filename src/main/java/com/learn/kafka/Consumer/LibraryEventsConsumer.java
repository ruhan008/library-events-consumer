package com.learn.kafka.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.learn.kafka.Services.LibraryEventService;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventsConsumer {

	@Autowired
	private LibraryEventService libraryEventService;
	
	@KafkaListener(topics = { "library-events" })
	public void onMessgae(ConsumerRecord<Integer, String> consumerRecord)
			throws JsonMappingException, JsonProcessingException {

		log.info("Consumer Record : {}", consumerRecord);
		libraryEventService.processLibraryEvent(consumerRecord);

	}

}
