# kafka-lite
## Description
kafka-lite is an exploratory systems programming project inspired by Apache Kafka.
The goal is to implement a minimal broker in order to better understand the design
and trade-offs of log-based storage, concurrency, and disk-backed systems.

The broker is written in C++, with supporting components (producer/consumer clients, controllers) planned in Go.

## Prerequisites
- C++ 20
- CMake 3.16
- Compiling must be done with gcc compiler, since other compilers don't support atomic swaps for shared pointers (use the appropriate values for the `CMAKE_C_COMPILER` and `CMAKE_CXX_COMPILER` flags)

## Current Status
The project is in an early stage. At present, only the storage engine of the broker
has been (partially) implemented. The focus so far has been on correctness and data
layout rather than feature completeness or performance tuning.

## Implemented components
### Log Storage Engine

The storage engine is based on an append-only log divided into segments.

Current features include:
- Append-only log with segmented files and rollover logic
- Support for multiple concurrent reader threads and a single writer thread
- Per-segment index for faster offset lookup
- Memory-mapped (`mmap`) index files for sealed (read-only) segments

### Concurrency Model
The storage engine supports multiple concurrent readers and a single writer.
Concurrency is managed primarily using atomics with acquire–release semantics,
minimizing the use of mutexes. No guarantees of lock-freedom are made; the focus
is on correctness and clear reasoning about memory visibility and ordering.


## Design Focus
This project prioritizes:
- Clear concurrency design (single writer, multiple readers)
- Explicit handling of on-disk data layout
- Reasoning about ordering guarantees and correctness

Performance optimization and fault tolerance are considered out of scope for now.

## Next Steps
- Add unit tests for the storage engine using GoogleTest
- Refine edge cases and correctness guarantees
- Implement the broker core responsible for managing reader and writer threads
- Add a minimal TCP-based network layer for client communication using asynchronous I/O (planned with Boost.Asio)
