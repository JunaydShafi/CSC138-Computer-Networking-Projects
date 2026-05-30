# CSC138-Computer-Networking-Projects
TCP, UDP, and HTTP networking projects implemented in Java, including a threaded job queue server, reliable UDP file transfer receiver, and publish-subscribe message broker.

A collection of networking projects completed in CSC 138 (Computer Networking) at California State University, Sacramento.

These projects explore network programming, concurrency, protocol design, and distributed systems concepts through the implementation of TCP, UDP, and HTTP-based applications in Java.

## Technologies

* Java
* TCP/IP Networking
* UDP Networking
* HTTP/1.1
* Multithreading
* Thread Synchronization
* Concurrent Data Structures
* Socket Programming
* Network Protocol Design

---

## Project 1: Threaded Job Queue Server

Implemented a concurrent TCP server that accepts client connections and manages asynchronous jobs using worker threads.

### Key Concepts

* TCP socket programming
* Multi-threaded server architecture
* Thread synchronization
* Shared state management
* Client-server protocols

### Features

* Concurrent client connections
* Job submission and tracking
* Job status monitoring
* Job cancellation support
* Thread-safe job registry

---

## Project 2: Reliable UDP File Transfer

Implemented a reliable file transfer receiver over UDP capable of handling packet loss, duplicate packets, and out-of-order delivery.

### Key Concepts

* UDP communication
* Reliability protocols
* Packet acknowledgements
* Data integrity verification
* Concurrent transfer handling

### Features

* Duplicate packet detection
* Out-of-order packet reassembly
* MD5 checksum verification
* Multiple simultaneous file transfers
* Transfer timeout cleanup

---

## Project 3: HTTP Publish/Subscribe Broker

Implemented an HTTP/1.1 publish-subscribe message broker that allows clients to subscribe to topics, publish messages, and retrieve queued updates.

### Key Concepts

* HTTP protocol implementation
* Publish/Subscribe architecture
* Concurrent request processing
* Message distribution systems
* REST-style communication

### Features

* Topic subscriptions
* Message publishing
* Subscriber polling
* Multi-topic support
* Concurrent client handling
* CORS support

---

## Learning Outcomes

Through these projects I gained practical experience with:

* Network protocol implementation
* TCP and UDP socket programming
* Concurrent server development
* Thread synchronization and shared state management
* Distributed systems concepts
* Reliable data transfer mechanisms
* HTTP server design and request handling

These projects demonstrate the progression from transport-layer networking concepts through application-layer protocol implementation and distributed messaging systems.
