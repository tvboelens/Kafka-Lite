#include "../include/Log.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <shared_mutex>

namespace kafka_lite {
namespace broker {

Log::Log(const std::filesystem::path &dir, uint64_t max_segment_size)
    : dir_(dir), max_segment_size_(max_segment_size) {
    std::filesystem::create_directories(dir_);
    auto paths = determineSegmentFilepaths();
    if (!paths.empty())
        recover(paths);
    else {
        std::shared_ptr<Segment> segment = std::make_shared<Segment>(
            dir_, 0, max_segment_size_, SegmentState::Active);
        active_segment_.store(segment);
    }
}

std::vector<std::string> Log::determineSegmentFilepaths() {
    std::vector<std::string> segment_filenames;
    for (const auto &entry : std::filesystem::directory_iterator(dir_)) {
        if (entry.path().string().compare(entry.path().string().size() - 4, 4,
                                          ".log") == 0)
            segment_filenames.push_back(entry.path().filename());
    }
    return segment_filenames;
}

void Log::recover(const std::vector<std::string> &segment_filenames) {
    std::vector<uint64_t> base_offsets(segment_filenames.size());
    for (int i = 0; i < base_offsets.size(); ++i) {
        base_offsets[i] = std::stoll(
            segment_filenames[i].substr(0, segment_filenames[i].size() - 4),
            nullptr, 10);
    }
    std::sort(base_offsets.begin(), base_offsets.end());
    std::shared_ptr<Segment> segment;
    for (auto it = base_offsets.begin(); it != base_offsets.end(); ++it) {
        // delete index file, since we rebuild it during segment recovery
        auto index_fp = dir_ / (std::to_string(*it) + ".index");
        std::filesystem::remove(index_fp);
        segment = std::make_shared<Segment>(dir_, *it, max_segment_size_,
                                            SegmentState::Active);
        auto result = segment->recover();
        if (it + 1 == base_offsets.end() ||
            result != RecoveryResult::Recovered) {
            active_segment_.store(segment, std::memory_order_seq_cst);
            if (result != RecoveryResult::Recovered)
                break;
        } else {
            segment->seal();
            sealed_segments_.push_back(segment);
        }
    }
}

FetchResult Log::fetch(const FetchRequest &request) const {
    FetchResult result, temp_result;
    uint64_t curr_offset = request.offset;
    size_t curr_result_size, curr_max_bytes = request.max_bytes;
    std::shared_ptr<Segment> segment;
    do {
        segment = findSegment(curr_offset);
        temp_result = segment->read(request.offset, curr_max_bytes);
        curr_result_size = result.result_buf.size();
        curr_max_bytes -= temp_result.result_buf.size();
        result.result_buf.resize(curr_result_size +
                                 temp_result.result_buf.size());
        std::memcpy(result.result_buf.data() + curr_result_size,
                    temp_result.result_buf.data(),
                    temp_result.result_buf.size());
        curr_offset = segment->getPublishedOffset() + 1;
    } while (result.result_buf.size() < request.max_bytes &&
             segment != active_segment_.load(std::memory_order_acquire));
    return result;
}

uint64_t Log::append(const AppendData &data) {
    auto active_segment = active_segment_.load(std::memory_order_acquire);
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

    auto next_active_segment = std::make_shared<Segment>(
             dir_, new_base_offset, max_segment_size_, SegmentState::Active),
         sealed_segment = std::make_shared<Segment>(
             dir_, old_base_offset, new_base_offset - 1, max_segment_size_,
             SegmentState::Sealed);

    {
        std::unique_lock<std::shared_mutex> lock(sealed_segments_mutex_);
        sealed_segments_.push_back(sealed_segment);
    }
    /*
       Let previous active segment go out of scope. Once all reader threads
       are done no shared ptr with a reference to it will exist and the
       destructor will clean up. The atomic swap and implementation of
       findSegment ensure that no new thread will read from previous active
       segment.
    */
    auto previous_active_segment = active_segment_.exchange(
        next_active_segment, std::memory_order_release);
}

std::shared_ptr<Segment> Log::findSegment(uint64_t offset) const {
    /**/
    std::shared_lock<std::shared_mutex> lock(sealed_segments_mutex_);
    auto active_segment = active_segment_.load(std::memory_order_acquire);
    if (sealed_segments_.empty() || active_segment->getBaseOffset() <= offset)
        return active_segment;
    auto it = sealed_segments_.begin();
    while (it != sealed_segments_.end() &&
           it->get()->getBaseOffset() <= offset) {
        ++it;
    }
    return *(it - 1);
}
} // namespace broker
} // namespace kafka_lite
