#ifndef SEGMENT_H
#define SEGMENT_H

#include <atomic>
#include <future>
#include <string>
#include <vector>

#include "./AppendQueue.h"

enum class SegmentState {
	Sealed,
	Active
};



struct FetchResult
{
	int fd;
	int64_t offset;
	int64_t length;

	std::vector<uint8_t> result_buf;
};

class Index
{
	public:
		Index();
		size_t determineLogFileOffset(uint64_t offset);
		void append(uint64_t offset, size_t log_file_offset);
	private:
		int fd_;
};

class Segment
{
	public:
		Segment(/* args */);
		~Segment();

		FetchResult read(uint64_t offset, size_t max_bytes); // args
		uint64_t append(const uint8_t *data, size_t len);
		void flush();
		void close();
		void seal();

		bool isFull();
		SegmentState state;
	private:
		void verifyDataIntegrity(FetchResult &result);
		int log_fd_;
		Index index_file;
		std::string dir;
		std::atomic<uint64_t> published_offset; // public?
		std::atomic<uint64_t> published_size; // public?
		uint64_t max_size_;
};

#endif
