package com.learn.kafka.Services;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.Models.LibraryEvent;
import com.learn.kafka.Repositories.LibraryEventRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class LibraryEventService {

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private LibraryEventRepository libraryEventRepository;

	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;

	public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord)
			throws JsonMappingException, JsonProcessingException {
		LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
		log.info("libraryEvent : {}", libraryEvent);
		if (libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 000) {
			throw new RecoverableDataAccessException("Temporary Network Access issue");
		}

		switch (libraryEvent.getLibraryEventType()) {
		case New:
			save(libraryEvent);
			break;
		case Update:
			validate(libraryEvent);
			save(libraryEvent);
			break;
		default:
			log.info("invalid library event type");
		}
	}

	private void validate(LibraryEvent libraryEvent) {
		if (libraryEvent.getLibraryEventId() == null) {
			throw new IllegalArgumentException("Library Event Id is missing");
		}

		Optional<LibraryEvent> optionalLibraryEvent = libraryEventRepository.findById(libraryEvent.getLibraryEventId());
		if (!optionalLibraryEvent.isPresent()) {
			throw new IllegalArgumentException("Not a valid library event id");
		}
		log.info("Validation is successful for the library event : {}", optionalLibraryEvent.get());
	}

	private void save(LibraryEvent libraryEvent) {
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventRepository.save(libraryEvent);
		log.info("Successfully persisted the library event {}", libraryEvent);

	}

	public void handleRecovery(ConsumerRecord<Integer, String> consumerRecord) {
		Integer key = consumerRecord.key();
		String value = consumerRecord.value();

		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);
			}

		});
	}

	private void handleFailure(Integer key, String value, Throwable ex) {
		log.error("Error sending the message and exception is {}", ex.getMessage());
		try {
			throw ex;
		} catch (Throwable throwable) {
			log.error("Error in OnFailure: {}", throwable.getMessage());
		}
	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("Message sent successfully for the key : {} and the value is {}, partition is {}", key, value,
				result.getRecordMetadata().partition());

	}

}
