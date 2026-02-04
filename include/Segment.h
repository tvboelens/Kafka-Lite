#ifndef SEGMENT_H
#define SEGMENT_H

#include <atomic>
#include <filesystem>
#include <vector>

namespace kafka_lite {
namespace broker {

#define INDEX_ENTRY_SIZE 12
#define OFFSET_SIZE 8
#define SEGMENT_HEADER_SIZE 4
#define FILE_POS_INDEX_SIZE 4

enum class SegmentState { Sealed, Active };

struct IndexFileEntry {
    uint64_t offset;
    uint32_t file_position;
};

struct FetchResult {
    int fd;
    int64_t offset;
    int64_t length;

    std::vector<uint8_t> result_buf;
};

class Index {
  public:
    Index(const std::filesystem::path &dir, uint64_t base_offset,
          SegmentState state);
    ~Index();
    IndexFileEntry determineClosestIndex(uint64_t offset);
    void append(const IndexFileEntry &entry);
    SegmentState state_;

  private:
    IndexFileEntry binarySearch(uint64_t offset, const char *buf,
                                uint64_t file_size);
    // IndexFileEntry binarySearchFromVector(uint64_t offset);
    std::filesystem::path dir_;
    const char *mmap_base_offset_;
    std::vector<uint8_t> data_;
    int fd_;
    std::atomic<uint64_t> published_size;
};

class Segment {
  public:
    Segment(const std::filesystem::path &dir, uint64_t base_offset,
            uint64_t max_size, SegmentState state);// Empty segment
    Segment(const std::filesystem::path &dir, uint64_t base_offset,
            uint64_t published_offset, uint64_t max_size, SegmentState state);// Nonempty segment
    ~Segment();

    FetchResult read(uint64_t offset, size_t max_bytes);
    uint64_t append(const uint8_t *data, uint32_t len);
    uint64_t getBaseOffset() { return base_offset_; };
    uint64_t getPublishedOffset() {
        return published_offset_.load(std::memory_order_acquire);
    }
    void flush();
    void seal();

    bool isFull();

  private:
    void init();
    void verifyDataIntegrity(FetchResult &result);
    uint32_t determineFilePosition(uint64_t offset);
    uint32_t determineFilePosition(uint64_t offset,
                                   const IndexFileEntry &entry);

    SegmentState state_;
    int log_fd_;
    Index index_file_;
    std::filesystem::path dir_;
    std::atomic<uint64_t> published_offset_; // public?
    std::atomic<uint64_t> published_size_;   // public?
    uint64_t max_size_, base_offset_;
};
} // namespace broker
} // namespace kafka_lite
#endif
