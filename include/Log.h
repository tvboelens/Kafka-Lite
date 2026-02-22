#ifndef LOG_H
#define LOG_H

#include "Segment.h"
#include <cstdint>
#include <filesystem>
#include <memory>
#include <shared_mutex>
#include <vector>

namespace kafka_lite {
namespace broker {
struct FetchRequest {
    uint64_t offset;
    size_t max_bytes;
};

struct AppendData {
    std::vector<uint8_t> data;
};

class Log {
  public:
    Log(const std::filesystem::path &dir, uint64_t max_segment_size)
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
    Log(const Log &other) = delete;
    Log &operator=(const Log &other) = delete;
    Log(Log &&other) = delete;
    Log &operator=(Log &&other) = delete;
    ~Log() = default; // Do I need more?
    FetchResult fetch(const FetchRequest &request) const;
    uint64_t append(const AppendData &data);
    void rollover();

  private:
    std::vector<std::string> determineSegmentFilepaths();
    void recover(const std::vector<std::string> &segment_filepaths);
    std::filesystem::path dir_;
    uint64_t max_segment_size_;
    std::shared_ptr<Segment> findSegment(uint64_t offset) const;
    std::vector<std::shared_ptr<Segment>> sealed_segments_;
    std::atomic<std::shared_ptr<Segment>> active_segment_;
    mutable std::shared_mutex sealed_segments_mutex_;
};
} // namespace broker
} // namespace kafka_lite
#endif
