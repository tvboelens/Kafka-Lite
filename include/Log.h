#ifndef LOG_H
#define LOG_H

#include "Segment.h"
#include "AppendQueue.h"
#include <filesystem>
#include <memory>

struct FetchRequest {
	uint64_t offset;
	size_t max_bytes;
};

class Log
{
	public:
		Log(/* args */);
		~Log();
		FetchResult read(const FetchRequest &request);
		uint64_t append(const AppendJob &job);
		void write() {};
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

Log::Log(/* args */)
{
}

Log::~Log()
{
}


#endif
