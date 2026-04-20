package com.danielprinz.udemy.books.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
@ExtendWith(VertxExtension.class)
public class JDBCMainVerticleTest {

  @Container
  public static PostgreSQLContainer container = new PostgreSQLContainer("postgres:17.9-alpine");

  static {
    container.withDatabaseName("books")
      .withUsername("postgres")
      .withPassword("secret");
  }

  @BeforeAll
  static void runMigrations() {
    Flyway flyway = Flyway.configure()
      .dataSource(container.getJdbcUrl(), container.getUsername(), container.getPassword())
      .load();
    flyway.migrate();
  }

  @BeforeEach
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) {
    JsonObject config = new JsonObject()
      .put("url", container.getJdbcUrl())
      .put("user", container.getUsername())
      .put("password", container.getPassword());

    DeploymentOptions options = new DeploymentOptions().setConfig(config);
    vertx.deployVerticle(new JDBCMainVerticle(), options)
      .onComplete(testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  void testGetAllBooks(Vertx vertx, VertxTestContext testContext) {
    WebClient client = WebClient.create(vertx);

    client.get(8888, "localhost", "/books")
      .as(BodyCodec.jsonArray())
      .send().onComplete(testContext.succeeding(response -> testContext.verify(() -> {
        assertEquals(200, response.statusCode());
        JsonArray books = response.body();
        assertNotNull(books);
        assertEquals(20, books.size());
        testContext.completeNow();
      })));
  }

  @Test
  void testCreateBook(Vertx vertx, VertxTestContext testContext) {
    WebClient client = WebClient.create(vertx);
    JsonObject newBook = new JsonObject()
      .put("isbn", "123456789")
      .put("title", "Testcontainers Book");

    client.post(8888, "localhost", "/books")
      .as(BodyCodec.jsonObject())
      .sendJsonObject(newBook).onComplete(testContext.succeeding(response -> testContext.verify(() -> {
        assertEquals(201, response.statusCode());
        JsonObject created = response.body();
        assertEquals("123456789", created.getString("isbn"));
        assertEquals("Testcontainers Book", created.getString("title"));
        testContext.completeNow();
      })));
  }

  @Test
  void testGetBookByIsbn(Vertx vertx, VertxTestContext testContext) {
    WebClient client = WebClient.create(vertx);

    client.get(8888, "localhost", "/books/9780134685991")
      .as(BodyCodec.jsonObject())
      .send().onComplete(testContext.succeeding(response -> testContext.verify(() -> {
        assertEquals(200, response.statusCode());
        JsonObject book = response.body();
        assertNotNull(book);
        assertEquals("Effective Java", book.getString("title"));
        testContext.completeNow();
      })));
  }

  @Test
  void testGetBookByIsbnNotFound(Vertx vertx, VertxTestContext testContext) {
    WebClient client = WebClient.create(vertx);

    client.get(8888, "localhost", "/books/1")
      .as(BodyCodec.jsonObject())
      .send().onComplete(testContext.succeeding(response -> testContext.verify(() -> {
        assertEquals(404, response.statusCode());
        assertEquals("Book not found!", response.body().getString("error"));
        testContext.completeNow();
      })));
  }

  @Test
  void testUpdateBook(Vertx vertx, VertxTestContext testContext) {
    WebClient client = WebClient.create(vertx);
    JsonObject payload = new JsonObject()
      .put("isbn", 9780132350884L)
      .put("title", "Clean Code (Updated)");

    client.put(8888, "localhost", "/books/9780132350884")
      .as(BodyCodec.jsonObject())
      .sendJsonObject(payload).onComplete(testContext.succeeding(response -> testContext.verify(() -> {
        assertEquals(200, response.statusCode());
        JsonObject updated = response.body();
        assertEquals(9780132350884L, updated.getLong("isbn"));
        assertEquals("Clean Code (Updated)", updated.getString("title"));
        testContext.completeNow();
      })));
  }

  @Test
  void testUpdateBookIsbnMismatch(Vertx vertx, VertxTestContext testContext) {
    WebClient client = WebClient.create(vertx);
    JsonObject payload = new JsonObject()
      .put("isbn", 123L)
      .put("title", "Whatever");

    client.put(8888, "localhost", "/books/9780134685991")
      .as(BodyCodec.jsonObject())
      .sendJsonObject(payload).onComplete(testContext.succeeding(response -> testContext.verify(() -> {
        assertEquals(400, response.statusCode());
        assertEquals("ISBN does not match!", response.body().getString("error"));
        testContext.completeNow();
      })));
  }

  @Test
  void testUpdateBookNotFound(Vertx vertx, VertxTestContext testContext) {
    WebClient client = WebClient.create(vertx);
    JsonObject payload = new JsonObject()
      .put("isbn", 42L)
      .put("title", "Missing");

    client.put(8888, "localhost", "/books/42")
      .as(BodyCodec.jsonObject())
      .sendJsonObject(payload).onComplete(testContext.succeeding(response -> testContext.verify(() -> {
        assertEquals(404, response.statusCode());
        assertEquals("Book not found!", response.body().getString("error"));
        testContext.completeNow();
      })));
  }
}
