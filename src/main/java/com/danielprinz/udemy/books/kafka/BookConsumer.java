package com.danielprinz.udemy.books.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danielprinz.udemy.books.Book;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.kafka.client.consumer.KafkaConsumer;

public class BookConsumer extends AbstractVerticle {

  private static final Logger LOG = LoggerFactory.getLogger(BookConsumer.class);
  public static final String BOOK_OUTPUT = "book-output";

  private final String bootstrapServers;

  public BookConsumer(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  @Override
  public void start(final Promise<Void> startPromise) {
    final Map<String, String> config = createConsumerConfig();

    KafkaConsumer.<Long, Book>create(vertx, config)
      .exceptionHandler(err -> LOG.error("consumer error:", err))
      .handler(record -> {
        LOG.info("> {}", record);
        vertx.eventBus().publish(BOOK_OUTPUT, record.value());
      })
      .subscribe(BookProducer.BOOKS_TOPIC, startPromise)
    ;
  }

  private Map<String, String> createConsumerConfig() {
    final Map<String, String> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BookSerde.class.getName());
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "book-cg");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    return config;
  }
}
