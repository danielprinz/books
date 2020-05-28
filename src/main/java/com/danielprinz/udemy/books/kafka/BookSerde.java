package com.danielprinz.udemy.books.kafka;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.danielprinz.udemy.books.Book;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public class BookSerde implements Serializer<Book>, Deserializer<Book> {

  @Override
  public Book deserialize(final String topic, final byte[] data) {
    if (data == null) {
      return null;
    }
    return new JsonObject(Buffer.buffer(data)).mapTo(Book.class);
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    // noop
  }

  @Override
  public byte[] serialize(final String topic, final Book data) {
    if (data == null) {
      return null;
    }
    return JsonObject.mapFrom(data).toBuffer().getBytes();
  }

  @Override
  public void close() {

  }
}
