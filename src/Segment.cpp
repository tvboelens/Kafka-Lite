#include "../include/Segment.h"
#include <unistd.h>
#include <bit>
#include <limits>

constexpr bool is_big_endian() {
	return std::endian::native == std::endian::big;
}

constexpr std::uint32_t byteswap32(std::uint32_t value)
{
	return ((value & 0x000000FF) << 24) |
		   ((value & 0x0000FF00) << 8)  |
		   ((value & 0x00FF0000) >> 8)  |
		   ((value & 0xFF000000) >> 24);
}

constexpr std::uint64_t byteswap64(std::uint64_t value)
{
	return ((value & 0x00000000000000FF) << 56) |
		   ((value & 0x000000000000FF00) << 40) |
		   ((value & 0x0000000000FF0000) << 24) |
		   ((value & 0x00000000FF000000) << 8)  |
		   ((value & 0x000000FF00000000) >> 8)  |
		   ((value & 0x0000FF0000000000) >> 24) |
		   ((value & 0x00FF000000000000) >> 40) |
		   ((value & 0xFF00000000000000) >> 56);
}

bool write_u32_le(int fd, u_int32_t value) {
	if (is_big_endian())
		value = byteswap32(value);

	ssize_t bytes_written = write(fd, &value, sizeof(value));
	return bytes_written == sizeof(value);
}

bool write_u64_le(int fd, u_int64_t value)
{
	if (is_big_endian())
		value = byteswap64(value);

	ssize_t bytes_written = write(fd, &value, sizeof(value));
	return bytes_written == sizeof(value);
}

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
		checksum: 32 bits (once we include it)
		payload: variable length
	*/
	uint32_t offset_file_position = determineFilePosition(offset);

	size_t len;
	if (segment_size-offset_file_position > max_bytes) {
		uint64_t current_offset = pub_offset;
		IndexFileEntry entry;
		do {
			entry = index_file.determineLogFileOffset(current_offset);
			current_offset = entry.offset - 1;
		} while (entry.file_position - offset_file_position > max_bytes);

		uint32_t next_file_position = determineFilePosition(entry.offset + 1), current_file_position = entry.file_position;
		current_offset = entry.offset + 1;
		
		while (next_file_position - offset_file_position < max_bytes) {
			++current_offset;
			current_file_position = next_file_position;
			next_file_position = determineFilePosition(current_offset, entry);
		}
		len = current_file_position - offset_file_position; // +1?
	}
	else 
		len = segment_size - offset_file_position;

	// use pread for thread safety
	result.result_buf.resize(len);
	size_t bytes_read = pread(log_fd_, result.result_buf.data(), len, offset_file_position);

	if (state == SegmentState::Active)
		verifyDataIntegrity(result);

	return result;
}

uint64_t Segment::append(const uint8_t *data, u_int32_t len)
{
	// Write to log files, maybe need to handle write errors
	off_t pos = lseek(log_fd_, 0, SEEK_CUR);
	// lseek might fail, check error and reopen file if necessary or throw exception

	if (pos > static_cast<off_t>(std::numeric_limits<uint32_t>::max())) {
		throw std::overflow_error("File position does not fit in uint32_t.");
	}

	if (!write_u32_le(log_fd_, len))
		throw std::ios_base::failure("Failed to write length to log file.");
	/*
		Future feature: Create and write checksum -> use crc32, there should be libraries that do this (boost::crc?)
	*/
	uint64_t bytes_written = write(log_fd_, data, len);

	// Increase published offset in thread safe way and write to index file
	uint64_t offset = published_offset.fetch_add(1, std::memory_order_release);
	IndexFileEntry index_data;
	index_data.offset = offset;
	index_data.file_position = static_cast<uint32_t>(pos);
	index_file.append(index_data);

	uint64_t new_size = published_size.fetch_add(bytes_written, std::memory_order_release);
	return offset;
}

uint32_t Segment::determineFilePosition(uint64_t offset){
	IndexFileEntry entry = index_file.determineLogFileOffset(offset);
	return determineFilePosition(offset, entry);
}

uint32_t Segment::determineFilePosition(uint64_t offset, const IndexFileEntry &entry)
{
	uint64_t current_offset = entry.offset;
	uint32_t len, current_file_pos = entry.file_position;
	while (current_offset < offset) {
		++current_offset;
		size_t bytes_read = pread(log_fd_, &len, 4, current_file_pos);
		if (bytes_read != 4)
			throw std::ios_base::failure("Failed to read length bytes from log file.");
		if (is_big_endian())
			len = byteswap32(len);
		current_file_pos += len + 4; // Later + 8 if we include the checksum
	}
	return current_file_pos;
}

void Segment::verifyDataIntegrity(FetchResult &result) {
	/*
		For later when we include the checksums.
		This will verify the integrity of a record.
		If a record is corrupted, then the result is truncated to hold all the records up until the corrupted one.
	*/
}

IndexFileEntry Index::determineLogFileOffset(uint64_t offset) {
	return {};
}

void Index::append(const IndexFileEntry &data) {
	if (!write_u64_le(fd_, data.offset))
		throw std::ios_base::failure("Failed to write offset to index file.");

	if (!write_u32_le(fd_, data.file_position))
		throw std::ios_base::failure("Failed to write file position to index file.");

	uint64_t size = published_size.fetch_add(12, std::memory_order_release);
}
