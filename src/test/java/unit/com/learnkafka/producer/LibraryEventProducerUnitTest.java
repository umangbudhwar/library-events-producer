package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

    @InjectMocks
    private LibraryEventProducer eventProducer;

    @Mock
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void sendLibraryEvent_Approach2_failure() throws JsonProcessingException, ExecutionException, InterruptedException {
        Book book = Book.builder()
                .bookId(null)
                .bookAuthor(null)
                .bookName("Kafka Using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        CompletableFuture completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new RuntimeException("Exception Calling Kafka"));

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(completableFuture);
        assertThrows(Exception.class, () -> eventProducer.sendLibraryEvent_Approach2(libraryEvent).get());
    }

    @Test
    public void sendLibraryEvent_Approach2_success() throws JsonProcessingException, ExecutionException, InterruptedException {
        Book book = Book.builder()
                .bookId(null)
                .bookAuthor(null)
                .bookName("Kafka Using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        String record = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("library-events", libraryEvent.getLibraryEventId(), record);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1),1, 1, 342, 1, 2);
        SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, recordMetadata);
        CompletableFuture completableFuture = new CompletableFuture<>();
        completableFuture.complete(sendResult);

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(completableFuture);
        CompletableFuture<SendResult<Integer, String>> completableFutureResult = eventProducer.sendLibraryEvent_Approach2(libraryEvent);

        SendResult<Integer, String> sendResult1 = completableFutureResult.get();

        assert sendResult1.getRecordMetadata().partition() == 1;

    }
}
