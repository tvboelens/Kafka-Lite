# kafka-lite
## Description
kafka-lite is an exploratory systems programming project inspired by Apache Kafka.
The goal is to implement a minimal broker in order to better understand the design
and trade-offs of log-based storage, concurrency, and disk-backed systems.
kafka-lite is currently an end-to-end prototype consisting of a segmented log storage engine, a custom TCP protocol, an asynchronous Boost.Asio networking layer, and a minimal client used for integration testing.

The broker is written in C++, with supporting components (producer/consumer clients, controllers) planned in Go and Rust.

## Getting started
### Prerequisites
- C++ 20
- CMake 4.0
- Boost 1.90 (Boost.Asio for networking and Boost.CRC for checksums)
- Linux or POSIX-compliant OS, since POSIX calls like `pread` (and many others) are used in the code.

## Building
Run 
```
cmake -B {build_dir} -S {repo_dir}/cpp -DCMAKE_BUILD_TYPE=Release
```
(or use build type Debug if you want) to generate the Makefiles, move to the build directory, and run `make` to build the targets `Broker` and `TestSuite`. Optionally use the `-j` flag when running `make` to speed up compilation.

Including Boost might prove to be tricky. `CMakeLists.txt` is written for a Boost installation that has Boost.Asio and Boost.CRC as header-only libraries. This is hopefully the case on all platforms, but if not you may need to alter `CMakeLists.txt` to check for certain components in the `find_package` command and might also need to directly link against Boost.Asio and Boost.CRC.

## Current Status
The broker is close to being functional, all of the core components are implemented and most of them tested.

### Implemented and tested components
#### Log Storage Engine
- Append-only log with segmented files and rollover logic
- Support for multiple concurrent reader threads and a single writer thread
- A concurrency model that minimizes locking (i.e. mutex use), instead primarily synchronizing through atomics with acquire-release semantics.
- Per-segment index for faster offset lookup
- Memory-mapped (`mmap`) index files for sealed (read-only) segments
- Crash recovery at startup: Invalid records are truncated

#### Boost.Asio networking layer
- Asynchronous TCP I/O (callback style)
- Network layer tests via loopback and a client

#### Minimal test client
- Synchronous API (also via Boost.Asio)
- Used for integration testing
- No cli yet
- Exercises the custom TCP protocol end-to-end

#### Custom TCP protocol
- Serialization/deserialization of TCP requests and responses
- Protocol validation tests

## Next Steps
### Missing pieces for Broker functionality
- Test and validate BrokerCore
- Add multithreaded tests for BrokerCore and the storage layer
- Perform stress/endurance testing
- Add configuration support
- Finalize broker startup/runtime behavior

### Other features
- Client cli
- Structured logging
- Metrics and metadata

### Performance roadmap (after Broker is functional)
- Baseline throughput and latency benchmarks
- Batched appends to reduce syscall overhead
- sendfile-based fetch responses for zero-copy transfers
- Handle backpressure
- Sparse indexing
- Multiple Boost.Asio I/O threads (this could be enabled earlier)
- Investigating whether certain memcpy operations can be avoided
- Profile and optimize hot paths
