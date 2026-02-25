# kafka-lite
## Description
kafka-lite is an exploratory systems programming project inspired by Apache Kafka.
The goal is to implement a minimal broker in order to better understand the design
and trade-offs of log-based storage, concurrency, and disk-backed systems.

The broker is written in C++, with supporting components (producer/consumer clients, controllers) planned in Go.

## Getting started
### Prerequisites
- C++ 20
- CMake 4.0
- Boost 1.90 (Boost.Asio for networking and Boost.CRC for checksums)
- Linux or POSIX-compliant OS, since POSIX calls like `pread` (and many others) are used in the code.
- Compiling must be done with gcc compiler, since other compilers don't support atomic swaps for shared pointers (use the appropriate values for the `CMAKE_C_COMPILER` and `CMAKE_CXX_COMPILER` flags)

## Building
Run 
```
cmake -B build_dir -S repo_dir
```
to build the targets `Broker` and `TestSuite`. As of now `Broker` is not fully functional yet, but the `TestSuite` is.

As mentioned above, it may be necessary to add the flags 
```
-DCMAKE_CXX_COMPILER=path_to_gcc_compiler
```
and similarly for the C compiler if you default compiler is not gcc. Alternatively, alter `CMakeLists.txt` to include the path to your gcc compilers.

Including Boost might prove to be tricky. `CMakeLists.txt` is written for a Boost installation that has Boost.Asio and Boost.CRC as header-only libraries. This is hopefully the case on all platforms, but if not you may need to alter `CMakeLists.txt` to check for certain components in the `find_package` command and might also need to directly link against Boost.Asio and Boost.CRC.

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
- Crash recovery at startup: Invalid records are truncated

### Testing
The following features are tested
- Single threaded writes to log segments
- Single threaded reads from log segments
- Segment rollover without concurrent reads
- Single threaded writes to index
- Single threaded lookups from index
- Multiple segment recovery when files are complete (i.e. no corruption or partial writes)
- Single segment recovery in case of partial write of last record


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
- Expand unit tests for crash recovery
- Write tests for multithreaded scenarios
- Finish the broker core responsible for managing reader and writer threads
- Add a minimal TCP-based network layer for client communication using asynchronous I/O (planned with Boost.Asio)
