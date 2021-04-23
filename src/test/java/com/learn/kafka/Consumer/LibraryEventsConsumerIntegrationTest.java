package com.learn.kafka.Consumer;

import static org.mockito.ArgumentMatchers.isA;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.log;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.forwardedUrl;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.Models.Book;
import com.learn.kafka.Models.LibraryEvent;
import com.learn.kafka.Models.LibraryEventType;
import com.learn.kafka.Repositories.LibraryEventRepository;
import com.learn.kafka.Services.LibraryEventService;

@SpringBootTest
@EmbeddedKafka(topics = { "library-events" }, partitions = 3)
@TestPropertySource(properties = { "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}" })
class LibraryEventsConsumerIntegrationTest {

	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;

	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;

	@Autowired
	KafkaListenerEndpointRegistry endpointRegistry;

	@SpyBean
	LibraryEventsConsumer libraryEventsConsumer;

	@SpyBean
	LibraryEventService libraryEventService;

	@Autowired
	private LibraryEventRepository libraryEventRepository;

	@Autowired
	private ObjectMapper objectMapper;

	@BeforeEach
	void setUp() {
		for (MessageListenerContainer messageListenerContainer : endpointRegistry.getAllListenerContainers()) {
			ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
		}
	}

	@AfterEach
	void tearDown() {
		libraryEventRepository.deleteAll();
	}

	@Test
	public void publishNewLibraryEvent()
			throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		// given
		String jsonString = "{\"libraryEventId\":null,\"libraryEventType\":\"New\",\"book\":{\"bookId\":123,\"bookName\":\"Integration Tests for kafka - Post Method\",\"bookAuthor\":\"Ruhan\"}}";
		kafkaTemplate.sendDefault(jsonString).get();

		// when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		// then
		verify(libraryEventsConsumer, times(1)).onMessgae(isA(ConsumerRecord.class));
		verify(libraryEventService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

		List<LibraryEvent> libraryEventList = libraryEventRepository.findAll();
		assert libraryEventList.size() == 1;
		libraryEventList.forEach(libraryEvent -> {
			assert libraryEvent.getLibraryEventId() != null;
			assertEquals(123, libraryEvent.getBook().getBookId());
		});
	}

	@Test
	public void publishUpdateLibraryEvent()
			throws JsonMappingException, JsonProcessingException, InterruptedException, ExecutionException {
		// given
		String jsonString = "{\"libraryEventId\":null,\"libraryEventType\":\"New\",\"book\":{\"bookId\":123,\"bookName\":\"Integration Tests for kafka - Post Method\",\"bookAuthor\":\"Ruhan\"}}";
		LibraryEvent libraryEvent = objectMapper.readValue(jsonString, LibraryEvent.class);
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventRepository.save(libraryEvent);
		// publish the update library event
		Book updatedBook = Book.builder().bookId(123).bookName("Integration Tests for kafka - Post Method - 2.0")
				.bookAuthor("Ruhan").build();
		libraryEvent.setLibraryEventType(LibraryEventType.Update);
		libraryEvent.setBook(updatedBook);
		String updatedJsonString = objectMapper.writeValueAsString(libraryEvent);
		System.out.println(updatedJsonString);
		kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJsonString).get();
		// when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		// then
//		verify(libraryEventsConsumer, times(1)).onMessgae(isA(ConsumerRecord.class));
//		verify(libraryEventService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

		LibraryEvent persistedLibraryEvent = libraryEventRepository.findById(libraryEvent.getLibraryEventId()).get();
		assertEquals("Integration Tests for kafka - Post Method - 2.0", persistedLibraryEvent.getBook().getBookName());
	}

	@Test
	void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId()
			throws JsonProcessingException, InterruptedException, ExecutionException {
		// given
		Integer libraryEventId = 123;
		String json = "{\"libraryEventId\":" + libraryEventId
				+ ",\"libraryEventType\":\"Update\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
		System.out.println(json);
		kafkaTemplate.sendDefault(libraryEventId, json).get();
		// when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		verify(libraryEventsConsumer, atLeast(1)).onMessgae(isA(ConsumerRecord.class));
		verify(libraryEventService, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));

		Optional<LibraryEvent> libraryEventOptional = libraryEventRepository.findById(libraryEventId);
		assertFalse(libraryEventOptional.isPresent());
	}

	@Test
	void publishModifyLibraryEvent_Null_LibraryEventId()
			throws JsonProcessingException, InterruptedException, ExecutionException {
		// given
		Integer libraryEventId = null;
		String json = "{\"libraryEventId\":" + libraryEventId
				+ ",\"libraryEventType\":\"Update\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
		kafkaTemplate.sendDefault(libraryEventId, json).get();
		// when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		verify(libraryEventsConsumer, times(1)).onMessgae(isA(ConsumerRecord.class));
		verify(libraryEventService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
	}

	@Test
	void publishModifyLibraryEvent_000_LibraryEventId()
			throws JsonProcessingException, InterruptedException, ExecutionException {
		// given
		Integer libraryEventId = 000;
		String json = "{\"libraryEventId\":" + libraryEventId
				+ ",\"libraryEventType\":\"Update\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
		kafkaTemplate.sendDefault(libraryEventId, json).get();
		// when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

//		verify(libraryEventsConsumer, times(3)).onMessgae(isA(ConsumerRecord.class));
		verify(libraryEventService, times(4)).processLibraryEvent(isA(ConsumerRecord.class));
		verify(libraryEventService, times(1)).handleRecovery(isA(ConsumerRecord.class));
	}
}
