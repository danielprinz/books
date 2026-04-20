package com.danielprinz.udemy.books.jdbc;

import com.danielprinz.udemy.books.Book;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

public class JDBCBookRepository {

  private Pool pool;

  public JDBCBookRepository(final Vertx vertx, final JsonObject config) {
    JDBCConnectOptions connectOptions = new JDBCConnectOptions()
      .setJdbcUrl(config.getString("url", "jdbc:postgresql://127.0.0.1/books"))
      .setUser(config.getString("user", "postgres"))
      .setPassword(config.getString("password", "secret"));
    PoolOptions poolOptions = new PoolOptions().setMaxSize(16);
    pool = JDBCPool.pool(vertx, connectOptions, poolOptions);
  }

  public Future<JsonArray> getAll() {
    return pool.query("SELECT * FROM books").execute()
      .map(rows -> {
        final JsonArray result = new JsonArray();
        for (Row row : rows) {
          result.add(row.toJson());
        }
        return result;
      });
  }

  public Future<Void> add(final Book bookToAdd) {
    final Tuple params = Tuple.of(bookToAdd.getIsbn(), bookToAdd.getTitle());
    return pool.preparedQuery("INSERT INTO books (isbn, title) VALUES (?, ?)").execute(params)
      .compose(rs -> rs.rowCount() == 1
        ? Future.succeededFuture()
        : Future.failedFuture(new IllegalStateException("Wrong update count on insert " + rs.rowCount())));
  }

  public Future<String> delete(final String isbn) {
    final Tuple params = Tuple.of(Long.parseLong(isbn));
    return pool.preparedQuery("DELETE FROM books where books.isbn = ?").execute(params)
      .map(rs -> rs.rowCount() == 0 ? null : isbn);
  }

  public Future<JsonObject> get(final String isbn) {
    final Tuple params = Tuple.of(Long.parseLong(isbn));
    return pool.preparedQuery("SELECT * FROM books WHERE books.isbn = ?").execute(params)
      .map(rs -> rs.size() == 0 ? null : rs.iterator().next().toJson());
  }

  public Future<JsonObject> update(final String isbn, final Book entry) {
    if (Long.parseLong(isbn) != entry.getIsbn()) {
      return Future.failedFuture(new IllegalArgumentException("ISBN does not match!"));
    }
    final Tuple params = Tuple.of(entry.getTitle(), entry.getIsbn());
    return pool.preparedQuery("UPDATE books SET title = ? WHERE books.isbn = ?").execute(params)
      .map(rs -> rs.rowCount() == 0 ? null : JsonObject.mapFrom(entry));
  }
}
