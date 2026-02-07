## Broker
This is the C++ part. Each broker manages exactly one log partition. We start with reading and writing from disk first and deal with networking later.
### Components
- AppendQueue
	- Queue for the jobs that need to be appended, needs to be thread safe (i.e. have a mutex)
- BrokerCore
	- Owns the append queue and Log/StorageEngine.
	- Spawns the thread that takes append jobs from the queue and calls `append()`.
	- Here there should be a function that handles the `AppendRequest`
		- This creates an `AppendJob`, does `std::future result = job.result.get_future()`, pushes the job on the queue and calls `result.get()`, afterwards returns the result.
	- For reading not quite sure yet, this will depend on the network layer, need further research.
- Networking layer
	- not quite clear yet where this should live
	- so lets start with an AppendRequest
		- A single handler function would parse the request into an AppendJob, 
		- The AppendJob consists of the data and a result of type `std::promise`, we can do `std::future result = job.result.get_future()`.
		- Then push the job onto append queue and call `result.get()`.
		- Once the result arrives we return it with the response to the client.
		- So when to set the value of the promise?
			- Without batching this is easy. Call `Segment::write`, receive the offset and set value.
			- With batching we would have to:
				- Use a condition variable to check size of data on queue together with a timeout
				- Then once cv has notified/timed out, grab all the jobs from the queue and write the batch.
				- I see two possibilities
					1. Write each record, store the offset, then after all records are done, call fsync and then set the value of the promise
					2. Or really write like Kafka: Batch has base offset and every record has delta offset. Method for writing a batch which returns the base offset. Then go through the list and set the result to base offset + delta offset. Call fsync before setting the value of course.
		I feel like this can live inside BrokerCore, since BrokerCore owns the AppendQueue, so it can have the handler function and pass this handler function to boost asio.
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
### Segment class
- The most sophisticated class, since it is responsible for almost all file operations (Index also has some, but is easier, since all its entries have the same length).
- Uses atomics and acquire-release semantics to achieve synchronization/thread-safety.
	- Size and published offset as atomics to ensure not reading past EOF or reading an offset that has not been written yet
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
	- The producer has already written/serialized the headers and the payload of the batch/record
	- If we use a fixed format for the record/batch we only need:
		1. length
		2. checksum
		3. Additionally some fixed headers from the TCP
	3. Then we can assign (base) offset and just directly write the payload to disk
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


## Orchestration (Go)
- Maintain connections with brokers
- Manage subscriptions and assign partitions to consumers within a group
- Manage partitions
- Leader election
- Handle faults (for example a broker dying)

