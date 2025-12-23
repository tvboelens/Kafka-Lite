#include "../include/Log.h"

FetchResult Log::read(const FetchRequest &request) {
	std::shared_ptr<Segment> segment = findSegment(request.offset);
	return segment->read(request.offset, request.max_bytes);
}

uint64_t Log::append(const AppendJob &job) {
	std::shared_ptr<Segment> active_segment = active_segment_.load(std::memory_order_acquire);
	uint64_t offset = active_segment->append(job.payload.data(), job.payload.size());
	if (active_segment->isFull())
		// optionally flush
		rollover();
	return offset;
}

void Log::rollover() {
	uint64_t old_base_segment = active_segment_.load(std::memory_order_relaxed)->getBaseOffset(), new_base_offset = active_segment_.load(std::memory_order_relaxed)->getPublishedOffset() + 1;

	std::shared_ptr<Segment> next_active_segment = std::make_shared<Segment>(dir_, new_base_offset, max_segment_size_, SegmentState::Active), 
		sealed_segment = std::make_shared<Segment>(dir_, old_base_segment, new_base_offset-1, max_segment_size_, SegmentState::Sealed);

	sealed_segments_.push_back(sealed_segment);
	std::shared_ptr<Segment> previous_active_segment = active_segment_.exchange(next_active_segment, std::memory_order_release);

	//while (previous_active_segment.use_count() > 1);
	//previous_active_segment->seal();
}
