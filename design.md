## Broker
This is the C++ part. Each broker manages exactly one log partition. We start with reading and writing from disk first and deal with networking later.
### Components
- AppendQueue
	- Queue for the jobs that need to be appended, needs to be thread safe (i.e. have a mutex)
- BrokerCore
	- Owns the append queue and Log/StorageEngine
	- Spawns the thread that takes append jobs from the queue and calls `append()`.
	- Here there should be a function that handles the `AppendRequest`
		- This creates an `AppendJob`, does `std::future result = job.result.get_future()`, pushes the job on the queue and calls `result.get()`, afterwards returns the result.
	- Reading also 
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
	- Fetches a record from the correct segment
	- Calls `write()/append()` from the active segment
	- Monitors which segment is active
	- Monitors if the active segment is full, closes it and rotates
		- main method here is `rollover()`
		- We use atomic shared pointer for active segment (need `gcc` compiler or more precisely `gcc libstdc++` for this).
		- We create a new segment `previous_active_segment` with sealed status, which opens the log and index file of the active segment in read-only mode.
		- We create a new segment that starts at the next offset in active mode.
		- Then we first add `previous_active_segment` to the vector of sealed segments so that future reading threads only read from here.
		- Then we atomically swap the new segment with the active segment.
		- The old active segment can simply go out of scope, since outside of `rollover()` only old reading threads hold a shared pointer to it, hence if all reading threads are ready the destructor will clean it up.
	- deletion of old segments?
	- crash recovery
	- metadata
- Segment class
	- This is the file I/O class
	- reads in thread safe way from the log and index file (use `pread`, later also allow `sendfile`).
	- writes to the log file and index file
	- holds filename, file dir, file size, maybe current offset (or current offset should be in log class, this seems to make more sense) and also base offset (for determining file names)
	- maybe index as separate class, where we put it inside the segment class
	- Need to track state (active vs sealed)
		- For active segments need to track published offset, i.e. readers can only read up to here, and published size
			- Need to be careful here, should use atomics and acquire-release semantics
		- For sealed segments we can allow reading everything
		- Maybe also recovering state
	- Need to do checksums so that data integrity can be verified, there should also be a method that does the verifying and truncates the result up to the last valid record. Verify only active segments and during recovery.
- Index class
	- Manages the index files, i.e. the map of an offset to the file position in the log file, this is encoded as pair of 64 bit unsigned int and 32 bit unsigned int.
	- It is probably fastest to do some kind of binary search to determine file position of given offset
		- But should first compare if the given offset is larger than largest offset in index file
		- Do as follows:
			- Left pointer at file start
			- Right pointer 12 bytes before EOF
			- Read 8 bytes to determine offset
			- Compare offset with given offset
				- If found read next 4 bytes and return these
			- So `R-L` should always be divisible by 12. So the new pointer should be moved to `L + 12*((R-L)/24)`
		- It might be smart to have an atomic for the file size here. We want to read 12 bytes before EOF, but if one thread reads while another writes EOF might not be well defined.
	- Might need to differentiate between index files of active and sealed segments
		- Index files of sealed segments can be mmapped and searched.
		- Index files of active segments need to keep their data in memory for reading.
	- Might have to handle partial writes, either if write was succesful but not whole buffer written or if there was an interrupt (check errno)



### Log partition
- We need to manage several segments, would probably be a good idea to have class for each segment. Every instance of the class is responsible for reading to/writing from disk.
	- We will need to use POSIX commands for this, both for performance reasons and for thread safety.
	- For writing we can use `write`, but we need to use `pread` for thread safety.
	- Might want to write a wrapper around a file descriptor with read and write functions.
- Then we need a class for managing the segments, this is the class that actually processes read and write requests.
	- For writing it tells the active segment to write and checks whether the segment is full, closes the segment if it is and starts a new log
	- For reading finds the segment containing the offset and then reads from it.
	- Should this also manage rotation etc? Or separate to a different class?
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

