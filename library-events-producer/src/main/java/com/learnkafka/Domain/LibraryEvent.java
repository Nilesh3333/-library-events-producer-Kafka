package com.learnkafka.Domain;

public record LibraryEvent(Integer libraryEventId,LibraryEventType libraryEventType,Book book) {
}
