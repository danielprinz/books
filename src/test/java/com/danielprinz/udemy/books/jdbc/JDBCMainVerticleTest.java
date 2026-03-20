package com.danielprinz.udemy.books.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@Testcontainers
@ExtendWith(VertxExtension.class)
public class JDBCMainVerticleTest {

  @Container
  public static PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:15-alpine")
      .withDatabaseName("books")
      .withUsername("postgres")
      .withPassword("secret");

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
    vertx.deployVerticle(new JDBCMainVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  void testGetAllBooks(Vertx vertx, VertxTestContext testContext) {
    WebClient client = WebClient.create(vertx);

    client.get(8888, "localhost", "/books")
        .expect(ResponsePredicate.SC_OK)
        .as(BodyCodec.jsonArray())
        .send(testContext.succeeding(response -> testContext.verify(() -> {
          JsonArray books = response.body();
          assertNotNull(books);
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
        .expect(ResponsePredicate.SC_CREATED)
        .as(BodyCodec.jsonObject())
        .sendJsonObject(newBook, testContext.succeeding(response -> testContext.verify(() -> {
          JsonObject created = response.body();
          assertEquals("123456789", created.getString("isbn"));
          assertEquals("Testcontainers Book", created.getString("title"));
          testContext.completeNow();
        })));
  }
}
