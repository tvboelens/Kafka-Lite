## Broker
This is the C++ part. Each broker manages exactly one log partition. We start with reading and writing from disk first and deal with networking later.
### Components
- AppendQueue
	- Queue for the jobs that need to be appended, needs to be thread safe (i.e. have a mutex)
- BrokerCore
	- Owns the append queue and Log/StorageEngine
	- Spawns the thread that takes append jobs from the queue and calls `append()`.
	- Reading also 
- Log class
	- Owns the segments
	- Fetches an event from the correct segment
	- Calls `write()/append()` from the active segment
	- Monitors which segment is active
	- Monitors if the active segment is full, closes it and rotates
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
	



### Log partition
- We need to manage several segments, would probably be a good idea to have class for each segment. Every instance of the class is responsible for reading to/writing from disk.
	- We will need to use POSIX commands for this, both for performance reasons and for thread safety.
	- For writing we can use `write`, but we need to use `pread` for thread safety.
	- Might want to write a wrapper around a file descriptor with read and write functions.
- Then we need a class for managing the segments, this is the class that actually processes read and write requests.
	- For writing it tells the active segment to write and checks whether the segment is full, closes the segment if it is and starts a new log
	- For reading finds the segment containing the offset and then reads from it.
	- Should this also manage rotation etc? Or separate to a different class?
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

