# Books
The books applications shows how build a complete web application.

The technologies used are:
* Vert.x backend
* Postgres data store
* Vue.js frontend

Full instructions how this application was build can be found in the Udemy course below.
Use the link with the coupon to get the best price:
https://www.udemy.com/course/reactive-web-applications-with-vertx-and-vuejs/?couponCode=GITHUB

## Docker Image Setup with local registry
1. `docker run -d -p 5000:5000 --restart=always --name registry registry:2`
2. `docker login localhost:5000` => `testuser/testpassword`
3. `mvn compile jib:build`
4. `docker run -p 8888:8888 localhost:5000/com.danielprinz.udemy-books:1.0.0-SNAPSHOT`
