#include "../include/Segment.h"
#include <unistd.h>

FetchResult Segment::read(uint64_t offset, size_t max_bytes) {
	// use acquire semantics to ensure synchronization with append
	uint64_t current_offset = offset, 
	         pub_offset = published_offset.load(std::memory_order_acquire), 
	         segment_size = published_size.load(std::memory_order_acquire);

	if (offset > pub_offset)
		return {};// May need to be changed if I want to use sendfile() in the future

	FetchResult result;
	/*
		len: 32 bits
		checksum: 32 bits
		payload: variable length
	*/
	size_t len, record_size, file_offset, base_offset = index_file.determineLogFileOffset(offset);
	if (segment_size-base_offset > max_bytes) {
		uint64_t current_offset = pub_offset;
		file_offset = index_file.determineLogFileOffset(pub_offset); // Here will need to check what to do if index not found
		while (file_offset - base_offset > max_bytes)
		{
			--current_offset;
			file_offset = index_file.determineLogFileOffset(current_offset);
		}
		len = file_offset - base_offset; // +1?
	}
	else 
		len = segment_size - base_offset;

	// use pread for thread safety
	result.result_buf.resize(len);
	size_t bytes_read = pread(log_fd_, result.result_buf.data(), len, base_offset);

	if (state == SegmentState::Active)
		verifyDataIntegrity(result);

	return result;
}

uint64_t Segment::append(const uint8_t *data, size_t len)
{
	// Write to log files, maybe need to handle write errors
	off_t pos = lseek(log_fd_, 0, SEEK_CUR);

	/*
		TODO:
		1. Include the length -> be careful of endianness here
		2. Create and write checksum -> use crc32, there should be libraries that do this
	*/

	uint64_t bytes_written = write(log_fd_, data, len);

	// Increase published offset in thread safe way and write to index file
	uint64_t offset = published_offset.fetch_add(1, std::memory_order_release);
	index_file.append(offset, pos);

	uint64_t new_size = published_size.fetch_add(bytes_written, std::memory_order_release);
	return offset;
}

void Segment::verifyDataIntegrity(FetchResult &result) {}

size_t Index::determineLogFileOffset(uint64_t offset) {
	return 0;
}

void Index::append(uint64_t offset, size_t log_file_offset) {
}
