# Threaded Job Queue Server

A multi-threaded TCP server implemented in Java that accepts client connections and manages asynchronous jobs through a thread-safe job queue.

## Features

* Concurrent client connections
* Job submission and tracking
* Job status monitoring
* Job cancellation support
* Thread-safe shared state management

## Concepts Demonstrated

* TCP socket programming
* Multi-threaded server design
* Thread synchronization
* Concurrent programming
* Client-server protocol implementation

## Run

```bash
java server.java <port>
```

Example:

```bash
java server.java 9099
```

