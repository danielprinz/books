package com.danielprinz.udemy.books;

import com.danielprinz.udemy.books.jdbc.JDBCMainVerticle;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.VerticleBase;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainVerticle extends VerticleBase {

  private static final Logger LOG = LoggerFactory.getLogger(MainVerticle.class);
  private final InMemoryBookStore store = new InMemoryBookStore();

  @Override
  public Future<?> start() throws Exception {
    LOG.debug("starting...");
    final Router books = Router.router(vertx);
    books.route().handler(BodyHandler.create());
    books.route("/*").handler(StaticHandler.create());

    //GET /books
    getAll(books);
    //GET /books/:isbn -> fetch one book
    getBookByISBN(books);
    //POST /books
    createBook(books);
    //PUT /books/:isbn
    updateBook(books);
    //DELETE /books/:isbn -> delete one book from in memory store
    deleteBook(books);

    registerErrorHandler(books);

    return vertx.createHttpServer().requestHandler(books).listen(8888)
      .onSuccess(http -> LOG.info("HTTP server started on port 8888"));
  }

  private void deleteBook(final Router books) {
    books.delete("/books/:isbn").handler(req -> {
      final String isbn = req.pathParam("isbn");
      final Book deletedBook = store.delete(isbn);
      if (null == deletedBook) {
        //Book not found
        req.response()
          .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
          .setStatusCode(HttpResponseStatus.NOT_FOUND.code())
          .end(new JsonObject().put("error", "Book not found!").encode());
      } else {
        //Deleted a book
        req.response()
          .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
          .end(JsonObject.mapFrom(deletedBook).encode());
      }
    });
  }

  private void getBookByISBN(final Router books) {
    books.get("/books/:isbn").handler(req -> {
      final String isbn = req.pathParam("isbn");
      req.response()
        .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
        .end(JsonObject.mapFrom(store.get(isbn)).encode());
    });
  }

  private void registerErrorHandler(final Router books) {
    books.errorHandler(500, event -> {
      LOG.error("Failed: ", event.failure());
      if (event.failure() instanceof IllegalArgumentException) {
        event.response()
          .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
          .setStatusCode(HttpResponseStatus.BAD_REQUEST.code())
          .end(new JsonObject().put("error", event.failure().getMessage()).encode());
        return;
      }
      event.response()
        .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
        .setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code())
        .end(new JsonObject().put("error", event.failure().getMessage()).encode());
    });
  }

  private void updateBook(final Router books) {
    books.put("/books/:isbn").handler(req -> {
      final String isbn = req.pathParam("isbn");
      final JsonObject requestBody = req.body().asJsonObject();
      final Book updatedBook = store.update(isbn, requestBody.mapTo(Book.class));
      //Return response
      req.response()
        .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
        .end(JsonObject.mapFrom(updatedBook).encode());
    });
  }

  private void createBook(final Router books) {
    books.post("/books").handler(req -> {
      // Read body
      final JsonObject requestBody = req.body().asJsonObject();
      System.out.println("Request Body: " + requestBody);
      // Store
      store.add(requestBody.mapTo(Book.class));
      // Return response
      req.response()
        .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
        .setStatusCode(HttpResponseStatus.CREATED.code())
        .end(requestBody.encode());
    });
  }

  private void getAll(final Router books) {
    books.get("/books").handler(req -> {
      // Return response
      req.response()
        .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
        .end(store.getAll().encode());
    });
  }

  static void main(String[] args) {
    Vertx vertx = Vertx.vertx();

    if (Boolean.parseBoolean(System.getenv("JDBC"))) {
      deployJDBCMainVerticle(vertx);
    } else {
      vertx.deployVerticle(new MainVerticle());
    }
  }

  private static void deployJDBCMainVerticle(Vertx vertx) {
    ConfigRetriever retriever = ConfigRetriever.create(vertx, new ConfigRetrieverOptions()
      .addStore(new ConfigStoreOptions()
        .setType("file")
        .setFormat("yaml")
        .setOptional(true)
        .setConfig(new JsonObject().put("path", "application.yml")))
      .addStore(new ConfigStoreOptions()
        .setType("file")
        .setFormat("yaml")
        .setOptional(true)
        .setConfig(new JsonObject().put("path", "src/main/resources/application.yml")))
      .addStore(new ConfigStoreOptions()
        .setType("env")));

    retriever.getConfig().onComplete(ar -> {
      if (ar.failed()) {
        LOG.error("Failed to load configuration", ar.cause());
        return;
      }
      final JsonObject config = ar.result();
      vertx.deployVerticle(new JDBCMainVerticle(),
        new DeploymentOptions().setConfig(config));
    });
  }
}
