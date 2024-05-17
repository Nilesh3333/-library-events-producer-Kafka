package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.Domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class LibraryEventProducer {

    @Value("${spring.kafka.topic}")
    public String topic;

    //KafkaTemplate<Integer, String> is a template for sending messages to Apache Kafka topics.This indicates the key and value types of the messages that will be sent to Kafka.
    //Check the LibraryEvent Class for the key and value
    private final KafkaTemplate<Integer,String> kafkaTemplate;

    //object mapper is a tool or library that facilitates the conversion of data between different representations, such as between objects in your application and a database, between objects and JSON/XML for web services, or between objects and other formats.
    private final ObjectMapper objectMapper;             // In our case we are converting object to string

    public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper){
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    //Approach 1
    //Asynchronous Call
    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvents(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent); // we are converting the object to string using ObjectMapping

        //completableFuture represents a future result of an asynchronous computation and provides a powerful way to deal with asynchronous programming in Java.
        //1.For the very first time Blocking call - get metadata about the kafka cluster
        //2.Send message happens - returns a CompletableFuture
        CompletableFuture<SendResult<Integer, String>> completableFuture= kafkaTemplate.send(topic,key,value);    //kafkaTemplate.send returns a completableFuture with SendResult<K, V>

        return completableFuture
                .whenComplete((sendResult,throwable) -> {
                    if(throwable != null){
                        handleFailure(key,value,throwable);
                    }
                    else{
                        handleSuccess(key,value,sendResult);
                    }
                });
    }

    //Approach 2
    //synchronous Call with the help of .get() method
    public SendResult<Integer, String> sendLibraryEvents_SyncronousCall(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent); // we are converting the object to string using ObjectMapping

        //1.For the very first time Blocking call - get metadata about the kafka cluster
        //2.Block and wait until the message is sent to the kafka with help of .get()
        SendResult<Integer, String> sendResult= kafkaTemplate.send(topic,key,value).get();    //kafkaTemplate.send returns a completableFuture with SendResult<K, V> and .get() will block and wait until it the message is sent to kafka
        handleSuccess(key,value,sendResult);
        return sendResult;
    }


    //Approach 3
    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvents_UsingProducerRecord(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent); // we are converting the object to string using ObjectMapping

        var producerRecord = buildProducerRecord(key,value);
        //ProducerRecord is an object that represents a message to be sent to a Kafka topic. It encapsulates all the information needed to send a message, including the topic name, partition (optional), key (optional), value, and headers (optional). It is part of the Kafka Producer API and is used to configure the messages that the producer sends to Kafka.
        CompletableFuture<SendResult<Integer, String>> completableFuture= kafkaTemplate.send(producerRecord);

        return completableFuture
                .whenComplete((sendResult,throwable) -> {
                    if(throwable != null){
                        handleFailure(key,value,throwable);
                    }
                    else{
                        handleSuccess(key,value,sendResult);
                    }
                });
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
        List<Header> recordHeader = List.of(new RecordHeader("Event-Souce","scanner".getBytes()));
        return new ProducerRecord<>(topic,null,key,value,recordHeader);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message Sent Successfull for {} and value {} ,Partition {}",key,value,sendResult.getRecordMetadata().partition());
    }

    private void handleFailure(Integer key, String value, Throwable throwable) {
        log.error("Error While Sending {} with value {} and the error is {}",key,value,throwable.getMessage());
    }

}
