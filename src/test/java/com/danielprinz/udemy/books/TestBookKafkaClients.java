package com.danielprinz.udemy.books;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danielprinz.udemy.books.kafka.BookConsumer;
import com.danielprinz.udemy.books.kafka.BookProducer;
import com.danielprinz.udemy.books.kafka.LocalMessageCodec;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;

import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
public class TestBookKafkaClients {

  private static final Logger LOG = LoggerFactory.getLogger(TestBookKafkaClients.class);

  @RegisterExtension
  public static final SharedKafkaTestResource KAFKA = new SharedKafkaTestResource()
    .withBrokers(3);

  @BeforeAll
  static void registerCodec(Vertx vertx) {
    vertx.eventBus().registerDefaultCodec(Book.class, new LocalMessageCodec<>(Book.class));
  }

  @Test
  void producingWorks(Vertx vertx, VertxTestContext context) {
    KAFKA.getKafkaTestUtils().createTopic(BookProducer.BOOKS_TOPIC, 1, (short) 1);
    vertx.deployVerticle(new BookProducer(KAFKA.getKafkaConnectString()), deployed -> {
      if (deployed.failed()) {
        context.failNow(deployed.cause());
      }
      // produce 10 records
      IntStream.range(0, 10).forEach(value ->
        vertx.eventBus().request(
          BookProducer.BOOK_INPUT,
          new Book(1, "test"),
          context.succeeding())
      );
      // verify records are in the topic
      final List<ConsumerRecord<byte[], byte[]>> all = KAFKA.getKafkaTestUtils()
        .consumeAllRecordsFromTopic(BookProducer.BOOKS_TOPIC);
      all.forEach(consumerRecord ->
        LOG.info("Consumed: {}", consumerRecord)
      );
      assertEquals(10, all.size());
      context.completeNow();
    });
  }

  @Test
  void consumingWorks(Vertx vertx, VertxTestContext context) {
    final String topicName = "consume-books";
    KAFKA.getKafkaTestUtils().createTopic(topicName, 1, (short) 1);

    final Checkpoint consumedAllMessages = context.checkpoint(10);
    vertx.eventBus().<Book>consumer(BookConsumer.BOOK_OUTPUT, msg -> {
      LOG.debug("Consumed a message {}", msg.body());
      consumedAllMessages.flag();
    });
    vertx.deployVerticle(new BookConsumer(KAFKA.getKafkaConnectString()), deployed -> {
      if (deployed.failed()) {
        context.failNow(deployed.cause());
      }
      // produce 10 records
      KAFKA.getKafkaTestUtils().produceRecords(10, topicName, 1);
    });
  }
}
