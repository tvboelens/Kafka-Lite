# C++
## Broker
This is the part that manages log partitions. Theoretically one broker could be responsible for multiple partitions over multiple topics. For simplicity we start with one topic and one partition.
### Components
- AppendQueue
    - Queue for the jobs that need to be appended, needs to be thread safe (i.e. have a mutex)
- BrokerCore
    - Owns the append queue and Log/StorageEngine.
    - Spawns the thread that takes append jobs from the queue and calls `append()`.
    - Either directly calls `Log::fetch` and invokes a callback or passes an append job (containing a callback) to the append queue and the writer thread calls `Log::append` and the writer thread invokes a callback.
- Networking layer (BrokerServer)
    - Owns BrokerCore, a reference to a `boost::asio::io_context` (which lives in `main`) and an instance of a `boost::asio::tcp::acceptor`.
    - Every connection is an instance of the `TcpConnection` class, where most of the logic lives.
        - One request per connection right now, still have to figure out how to reuse connections.
    - Logic is simple:
        - Parse tcp request
        - check if magic bytes are present, if not drop silently drop request
        - Then do some validation steps, check which type it is and invoke the corresponding method of `BrokerCore`.
        - Finally send response.
- Tcp protocol
    - Separated this from the network layer to make testing easy.
    - Has general `TcpRequest` and `TcpResponse` structs, as well as specialized request structs (`FetchRequest` and `AppendRequest`) together with (de)serializing functions and functions to specialize from general request to specialized requests.
    - Binary protocol will be described below.
- Log class
    - Owns the segments
    - Reads from and writes to correct segment
    - Calls `write()/append()` from the active segment
    - Monitors which segment is active and when to rotate/rollover
    - deletion of old segments
    - crash recovery
    - metadata
- Segment class
    - This is the file I/O class
    - reads in thread safe way from the log and index file (use `pread`, later also allow `sendfile`).
    - writes to the log file and index file
    - Need to do checksums so that data integrity can be verified, there should also be a method that does the verifying and truncates the result up to the last valid record. Verify only active segments and during recovery.
- Index class
    - Manages the index files, i.e. the map of an offset to the file position in the log file, this is encoded as pair of 64 bit unsigned int and 32 bit unsigned int.
    - Given `offset`, uses binary search to find largest index `idx` such that `idx <= offset`.
    - Differentiates between index files of active and sealed segments
        - Index files of sealed segments can be mmapped and searched and do not have an open file descriptor.
        - Index files of active segments have an open file descriptor (for writing) and keep their data in memory for reading.

### AppendQueue
This is a simple class that wraps a queue and a mutex.
Since I want to design for high throughput and do not expect long wait times, I implemented a `wait_and_pop(AppendJob&)` method that uses a condition variable to wait for the condition that the queue is nonempty. Can later refine this condition for example for batched writes The `push` method is as one would expects, i.e. lock mutex and push. 

### Log Class
- Apart from the rollover logic this class is not very sophisticated. It simply calls the `append` and `read` methods of the Segment class and returns the results.
- Active segment is an atomic shared pointer, sealed segments are stored as a vector of shared pointers.
- Potential rollover issue: We store shared pointers to the sealed segments in a vector and protect these with a mutex, which is locked both when reading and during rollover. If there are a lot of reader threads this could lead to the writer thread blocking for a long time before it acquires the lock. This should be inspected at some point.
-  A fix would be to use some state to signal that rollover is taking place. Then readers would have to wait on a condition variable depending on the state not being rollover or stopping. Here the writer thread should call `notify_all()`.
- I decided to use a shared_mutex. It seems to be platform independent whether writer starvation can occur, therefore I will have to monitor whether this happens and if so might have to find a library/implementation that does not starve the writer (maybe boost?).
- Well the previous part about only rollover logic being sophisticated is not quite true. We need to do crash recovery as well.
    - First need to discover all the files and then sort them along the offsets.
    - Then create a Segment instance, open the file and do crash recovery.
    - Do this until final file is reached.
    - For simplicity mark the final segment as active, let the normal write path do rollover logic if necessary
    - If a segment is corrupted, truncate it and discard all the segments following it. Reasoning is that a single broker node should care more about correctness and replication is responsible for retrieving the truncated data. Or in the event of disk corruption it might even be better to kill the node and let another one take over.
    - To keep things simple rebuild the index on recovery, i.e. delete the old index file and write a new one.
    - Crash recovery should always be done on startup
    - During recovery need to block read/write operations
### Segment class
- The most sophisticated class, since it is responsible for almost all file operations (Index also has some, but is easier, since all its entries have the same length).
- Uses atomics and acquire-release semantics to achieve synchronization/thread-safety.
    - Size and published offset as atomics to ensure not reading past EOF or reading an offset that has not been written yet
- Crash recovery
    - Check every record. If corrupted, truncate the file (use `ftruncate`). Use checksums for detecting corruption, but do note that the Segment does not do any checking on writing, this is the responsibility of BrokerCore or maybe the Server. So when testing we need to make sure that the data we use also has checksums.
    - Rebuild the index (see above)
    - So now the question is also how to construct a segment for recovery. Right now have constructor for active and one for sealed segment
        - So both constructors call init and in both cases this simply opens the file and stores the file size in published_size
        - Only difference is that the constructor for sealed segments takes the published offset as variable and setting status to sealed opens the file as read-only
        - So what is missing?
            - Using the sealed constructor is not possible: we do not know the published offset beforehand and we may need to modify the file (truncate)
            - using the active constructor is more advisable. the recovery method should then optionally change the status from active to sealed and if so reopen the file as read-only
            - Then for the index file, use `std::filesystem::remove` (maybe simply at the start) and construct the index file as active and append the last record we recovered (if interval mode do after `n` records)
            - So this means that also the index needs a `seal` method
- Following members
    - State: Active or Sealed, later maybe recovering
    - file descriptor
    - directory
    - published offset and size (atomics)
    - max_size and base_offset
    - instance of Index class

#### On Disk Format
- We can assume that the data is already serialized by producers before they send the data to the broker
- So I think regardless of batching we can assume the following:
    - length (record or batch length)
    - checksum (batch or record)
    - payload (including record/batch headers)
    - checksum is computed by the producer, needs to be verified by the broker before write
        - This simply involves recomputing the checksum and comparing with the received checksum
        - Question is where this should happen. I think pretty high up in the stack.
        - So Server passes to Core by pushing on the AppendQueue. And then we block the thread.
        - So I think checking in `Core::handleAppendRequest` would make sense, so that we can return early if we detect corruption or even already check in the caller.
- Payload structure
    - Record without batching
        - Don't need to specify anything further here right now, the storage engine should be agnostic to this structure.
    - Record with batching
        - length
        - offset delta (record offset - base offset)
        - Don't need to specify anything further, again the storage engine is agnostic w.r.t the rest
    - Batch
        - number of records
        - records themselves
        - last offset delta?
        - Kafka stores some other things, but these seem to be useful mostly for compaction (out of scope for now). Maybe attributes if I want to store `isTransactional` or `isControlBatch`, but unlikely for now.
- Kafka also has control records, unsure what to do with that
        
### Networking
- This should be done in a separate class/file
- This class listens to requests on a socket and relays read/write requests to the log class and responds
- Need to distinguish between reading and writing requests
    - Read requests can call a fetch directly, we will make sure reading is thread safe
    - write requests must lead to an AppendJob to be pushed on a queue. Appending will be single threaded to ensure it is strictly sequential.
- Use `boost::asio`
    - Main event loop is in `io_context`
    - `ip::tcp::acceptor` listens for incoming connections
    - `ip::tcp::socket` manages the socket
    - `io_context::run()` is compatible with multiple threads (how? multiple threads that call `run?` or `run()` itself can manage multiple threads?)
- Different kinds of requests
    - `AppendRequest`
    - `FetchRequest`
    - `HeartBeatRequest`
    - `ReplicaSyncRequest`
#### TCP request structure
Payload is sent as raw bytes (little endian order), in the headers ints are written in big endian order.
- Length header
- Checksum
- Correlation id (128 bit)
- type
    - Append
    - Fetch
    - Heartbeat
    - ReplicaSync
- protocol version (in case I decide to change the protocol)
- payload
#### TCP response structure
- length (big endian order)
- correlation id (0 if the request was missing one)
- return code
- payload (only if return code is 0)
#### Return codes
- 0 is OK
- Error codes
    - First bit used as flag to indicate whether system error (first bit equals 1) or bad request (first bit equals 0)
    - For system errors we only return the following:
        - not connected (which means shutdown) 0x80
        - I/O error 0x81
        - All other system errors are returned as 0xff
    - Request errors
        - missing correlation id 0x01,
        - unsupported version 0x02,
        - unknown request type 0x03,
        - unsupported flags 0x04,
    - TODO: Ignored because of backpressure

### Client classes and functions
- Function to generate TCP request from AppendRequest
- Function to generate TCP request from FetchRequest
- Function to generate checksum and prefix it to payload
- BrokerCore client for testing
    - Should be able to append and fetch from multiple threads and should record results per thread
- BrokerServer client


## Orchestration (Go)
- Maintain connections with brokers
- Manage subscriptions and assign partitions to consumers within a group
- Manage partitions
- Leader election
- Handle faults (for example a broker dying)

