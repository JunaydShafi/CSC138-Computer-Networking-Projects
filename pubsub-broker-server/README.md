# HTTP Publish-Subscribe Broker

An HTTP/1.1 publish-subscribe messaging server implemented in Java.

Clients can subscribe to topics, publish messages, and poll for queued updates through a simple HTTP API.

## Features

* Topic subscriptions
* Message publishing
* Subscriber polling
* Multi-topic support
* Concurrent client handling
* HTTP/1.1 request processing
* CORS support

## Concepts Demonstrated

* HTTP protocol implementation
* Publish/Subscribe architecture
* Concurrent server programming
* Message distribution systems
* REST-style API design

## Run

```bash
java pbs.java 8080
```

or

```bash
java pbs.java <port>
```

