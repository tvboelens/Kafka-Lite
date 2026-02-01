#include "../include/Log.h"
#include <cstdint>

FetchResult Log::fetch(const FetchRequest &request) {
    std::shared_ptr<Segment> segment = findSegment(request.offset);
    return segment->read(request.offset, request.max_bytes);
}

uint64_t Log::append(const AppendData &data) {
    std::shared_ptr<Segment> active_segment =
        active_segment_.load(std::memory_order_acquire);
    uint64_t offset =
        active_segment->append(data.data.data(), data.data.size());
    if (active_segment->isFull())
        // optionally flush
        rollover();
    return offset;
}

void Log::rollover() {
    uint64_t old_base_offset = active_segment_.load(std::memory_order_relaxed)
                                   ->getBaseOffset(),
             new_base_offset = active_segment_.load(std::memory_order_relaxed)
                                   ->getPublishedOffset() +
                               1;

    std::shared_ptr<Segment> next_active_segment = std::make_shared<Segment>(
                                 dir_, new_base_offset, max_segment_size_,
                                 SegmentState::Active),
                             sealed_segment = std::make_shared<Segment>(
                                 dir_, old_base_offset, new_base_offset - 1,
                                 max_segment_size_, SegmentState::Sealed);

    {
        std::lock_guard<std::mutex> lock(sealed_segments_mutex_);
        sealed_segments_.push_back(sealed_segment);
    }
    std::shared_ptr<Segment> previous_active_segment = active_segment_.exchange(
        next_active_segment, std::memory_order_release);
}

std::shared_ptr<Segment> Log::findSegment(uint64_t offset) {
    std::lock_guard<std::mutex> lock(sealed_segments_mutex_);
    std::vector<std::shared_ptr<Segment>>::const_iterator it =
        sealed_segments_.begin();
    while (it->get()->getBaseOffset() > offset && it != sealed_segments_.end()) {
        ++it;
    }
    if (it == sealed_segments_.end())
        return active_segment_.load(std::memory_order_acquire);
    return *it;
}
