# Reliable UDP File Transfer

A UDP-based file transfer receiver that implements reliability mechanisms on top of an unreliable transport protocol.

The receiver handles packet loss, duplicate packets, and out-of-order delivery while reconstructing files and verifying data integrity.

## Features

* Reliable file transfer over UDP
* Packet acknowledgment system
* Duplicate packet detection
* Out-of-order packet handling
* MD5 checksum verification
* Concurrent transfer support
* Transfer timeout cleanup

## Concepts Demonstrated

* UDP networking
* Reliability protocols
* Packet sequencing
* Data integrity verification
* Concurrent networking systems

## Run

```bash
java receiver.java <port>
```

Example:

```bash
java receiver.java 9099
```

