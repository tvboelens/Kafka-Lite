#ifndef LOG_H
#define LOG_H

#include "Segment.h"
#include <cstdint>
#include <filesystem>
#include <memory>
#include <vector>

struct FetchRequest {
	uint64_t offset;
	size_t max_bytes;
};

struct AppendData {
    std::vector<uint8_t> data;
};

class Log
{
	public:
		FetchResult fetch(const FetchRequest &request);
		uint64_t append(const AppendData &data);
		void rollover();

	private:
		std::filesystem::path dir_;
		uint64_t max_segment_size_;
		std::shared_ptr<Segment> findSegment(uint64_t offset) { return nullptr; };
		std::vector<std::shared_ptr<Segment>> sealed_segments_;
		std::atomic<std::shared_ptr<Segment>> active_segment_;
		std::mutex sealed_segments_mutex_;
		std::atomic_int active_segment_readers_;
};

#endif
