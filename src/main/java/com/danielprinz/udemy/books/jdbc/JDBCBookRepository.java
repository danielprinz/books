package com.danielprinz.udemy.books.jdbc;

import java.util.List;

import com.danielprinz.udemy.books.Book;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

public class JDBCBookRepository {

  private Pool pool;

  public JDBCBookRepository(final Vertx vertx) {
    final JsonObject config = vertx.getOrCreateContext().config();
    JDBCConnectOptions connectOptions = new JDBCConnectOptions()
      .setJdbcUrl(config.getString("url", "jdbc:postgresql://127.0.0.1/books"))
      .setUser(config.getString("user", "postgres"))
      .setPassword(config.getString("password", "secret"));
    PoolOptions poolOptions = new PoolOptions().setMaxSize(16);
    pool = JDBCPool.pool(vertx, connectOptions, poolOptions);
  }

  public Future<JsonArray> getAll() {
    final Promise<JsonArray> getAll = Promise.promise();
    pool.query("SELECT * FROM books").execute().onComplete(ar -> {
      if (ar.failed()){
        //Forward error
        getAll.fail(ar.cause());
        return;
      }
      //Return result
      final RowSet<Row> rows = ar.result();
      final JsonArray result = new JsonArray();
      for (Row row : rows) {
        result.add(row.toJson());
      }
      getAll.complete(result);
    });
    return getAll.future();
  }

  public Future<Void> add(final Book bookToAdd) {
    final Promise<Void> added = Promise.promise();
    final Tuple params = Tuple.of(bookToAdd.getIsbn(), bookToAdd.getTitle());
    pool.preparedQuery("INSERT INTO books (isbn, title) VALUES (?, ?)").execute(params).onComplete(ar -> {
      if (ar.failed()){
        //Forward error
        added.fail(ar.cause());
        return;
      }
      //Return failure if updated count is not 1
      if (ar.result().rowCount() != 1) {
        added.fail(new IllegalStateException("Wrong update count on insert " + ar.result().rowCount()));
        return;
      }
      //Return success
      added.complete();
    });
    return added.future();
  }

  public Future<String> delete(final String isbn) {
    final Promise<String> deleted = Promise.promise();
    final Tuple params = Tuple.of(Long.parseLong(isbn));
    pool.preparedQuery("DELETE FROM books where books.isbn = ?").execute(params).onComplete(ar -> {
      if (ar.failed()){
        //Forward error
        deleted.fail(ar.cause());
        return;
      }
      //Nothing was deleted
      if (ar.result().rowCount() == 0) {
        deleted.complete();
        return;
      }
      //Return deleted isbn
      deleted.complete(isbn);
    });
    return deleted.future();
  }
}
