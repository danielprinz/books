package com.danielprinz.udemy.books.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danielprinz.udemy.books.Book;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

public class BookProducer extends AbstractVerticle {

  private static final Logger LOG = LoggerFactory.getLogger(BookProducer.class);
  public static final String BOOK_INPUT = "book-input";
  public static final String BOOKS_TOPIC = "books-topic";

  private final String bootstrapServers;
  private KafkaProducer<Long, Book> producer;

  public BookProducer(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  @Override
  public void start(final Promise<Void> startPromise) {
    final Map<String, String> config = createProducerConfigs();

    producer = KafkaProducer.create(vertx, config);
    producer.exceptionHandler(err -> LOG.error("producer error:", err));

    vertx.eventBus().<Book>consumer(BOOK_INPUT, msg -> {
      final Book book = msg.body();
      final KafkaProducerRecord<Long, Book> record = KafkaProducerRecord.create(BOOKS_TOPIC, book.getIsbn(), book);
      producer.write(record, ar ->
        LOG.info("Produced: {} => {}", ar.succeeded(), record)
      );
    });
    startPromise.complete();
  }

  private Map<String, String> createProducerConfigs() {
    Map<String, String> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BookSerde.class.getName());
    //config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonObjectSerializer.class.getName());
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    return config;
  }

}
