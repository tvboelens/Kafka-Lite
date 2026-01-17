# kafka-lite
## Description
This project aims to build a mini version of Apache Kafka. The broker will be written in C++ and the other parts will be written in Go. I started this project since I wanted expand my skills in Go and at the same time learn systems programming in C++.

### Features
As of now only the storage engine of the broker has been (at least partly) implemented. Just like Kafka this uses an append-only log that is divided into segments. The log has the following features:
- Multiple reader threads and a single writer thread. 
- For faster lookup the log comes equipped with an index and for sealed segments (i.e. read-only) I use `mmap` on the index for faster reading. 
- The segment rollover logic is fully implemented.

The next step will be testing (using GoogleTest) the storage engine. After that the goal will be to first build the BrokerCore component that manages the read and write threads and finally the network layer.
