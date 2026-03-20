package com.danielprinz.udemy.books;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
public class TestMainVerticle {

  @BeforeEach
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) {
    vertx.deployVerticle(new MainVerticle(), testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  void testCreateBook(Vertx vertx, VertxTestContext testContext) {
    WebClient client = WebClient.create(vertx);
    JsonObject newBook = new JsonObject()
      .put("isbn", 111222333L)
      .put("title", "Learn Vert.x");

    client.post(8888, "localhost", "/books")
      .expect(ResponsePredicate.SC_CREATED)
      .as(BodyCodec.jsonObject())
      .sendJsonObject(newBook, testContext.succeeding(response -> testContext.verify(() -> {
        JsonObject created = response.body();
        assertEquals(111222333L, created.getLong("isbn"));
        assertEquals("Learn Vert.x", created.getString("title"));
        testContext.completeNow();
      })));
  }

  @Test
  void testGetAllBooks(Vertx vertx, VertxTestContext testContext) {
    WebClient client = WebClient.create(vertx);
    
    JsonObject newBook = new JsonObject().put("isbn", 999999999L).put("title", "Test Book");
    client.post(8888, "localhost", "/books")
      .sendJsonObject(newBook, testContext.succeeding(postResponse -> {
        client.get(8888, "localhost", "/books")
          .expect(ResponsePredicate.SC_OK)
          .as(BodyCodec.jsonArray())
          .send(testContext.succeeding(response -> testContext.verify(() -> {
            JsonArray books = response.body();
            assertEquals(3, books.size()); // 2 default + 1 new
            testContext.completeNow();
          })));
      }));
  }

  @Test
  void testUpdateBook(Vertx vertx, VertxTestContext testContext) {
    WebClient client = WebClient.create(vertx);
    
    JsonObject newBook = new JsonObject().put("isbn", 123L).put("title", "Old Title");
    client.post(8888, "localhost", "/books")
      .sendJsonObject(newBook, testContext.succeeding(postResponse -> {
        JsonObject updatedBook = new JsonObject().put("isbn", 123L).put("title", "New Title");
        client.put(8888, "localhost", "/books/123")
          .expect(ResponsePredicate.SC_OK)
          .as(BodyCodec.jsonObject())
          .sendJsonObject(updatedBook, testContext.succeeding(response -> testContext.verify(() -> {
            JsonObject returnedBook = response.body();
            assertEquals(123L, returnedBook.getLong("isbn"));
            assertEquals("New Title", returnedBook.getString("title"));
            testContext.completeNow();
          })));
      }));
  }

  @Test
  void testDeleteBook(Vertx vertx, VertxTestContext testContext) {
    WebClient client = WebClient.create(vertx);
    
    JsonObject newBook = new JsonObject().put("isbn", 456L).put("title", "To Be Deleted");
    client.post(8888, "localhost", "/books")
      .sendJsonObject(newBook, testContext.succeeding(postResponse -> {
        client.delete(8888, "localhost", "/books/456")
          .expect(ResponsePredicate.SC_OK)
          .as(BodyCodec.jsonObject())
          .send(testContext.succeeding(response -> testContext.verify(() -> {
            JsonObject deletedBook = response.body();
            assertEquals(456L, deletedBook.getLong("isbn"));
            testContext.completeNow();
          })));
      }));
  }
}
