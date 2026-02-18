#include "../include/Log.h"
#include "../include/Segment.h"
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

namespace kafka_lite {
namespace broker {

class StorageEngineTests : public ::testing::Test {
  private:
    std::filesystem::path dir_;

  public:
    std::filesystem::path getDir() { return dir_; }

  protected:
    void SetUp() override {
        dir_ = std::filesystem::current_path() / "Segment";
    }
    void TearDown() override;
};

void StorageEngineTests::TearDown() { std::filesystem::remove_all(dir_); }

TEST_F(StorageEngineTests, SegmentRW) {
    std::filesystem::path dir = getDir() / "SegmentRW";
    Segment segment(dir, 0, 32, SegmentState::Active);
    FetchResult result = segment.read(0, 32);
    ASSERT_TRUE(result.result_buf.empty());
    std::vector<uint8_t> data, result_buf;
    data.reserve(4);
    for (int i = 1; i < 5; ++i) {
        data.push_back(i);
    }
    uint64_t offset =
        segment.append(data.data(), data.size() * sizeof(uint8_t));
    result = segment.read(offset, 32);
    result_buf.reserve(data.size());
    for (auto it = result.result_buf.begin() + SEGMENT_HEADER_SIZE;
         it != result.result_buf.end(); ++it) {
        result_buf.push_back(*it);
    }
    ASSERT_EQ(result_buf, data);
    ASSERT_EQ(segment.getBaseOffset(), segment.getPublishedOffset());
};

TEST_F(StorageEngineTests, SegmentRWMultiple) {
    std::filesystem::path dir = getDir() / "SegmentRWMultiple";
    std::vector<uint8_t> data;
    std::vector<std::vector<uint8_t>> datav;
    datav.reserve(8);
    data.resize(4);
    std::vector<uint64_t> offsets;
    FetchResult result;
    {
        Segment segment(dir, 0, 32, SegmentState::Active);

        offsets.resize(8);
        data.reserve(4);
        for (int i = 0; i < 8; ++i) {
            for (int j = 0; j < 4; ++j) {
                data[j] = i * 4 + j;
            }
            datav.push_back(data);
            offsets[i] =
                segment.append(data.data(), data.size() * sizeof(uint8_t));
        }
        for (int i = 0; i < 8; ++i) {
            EXPECT_EQ(offsets[i], i);
        }
        EXPECT_EQ(segment.getPublishedOffset(), 7);
        EXPECT_TRUE(segment.isFull());
        result = segment.read(7, 256);
        ASSERT_EQ(result.result_buf.size(), 4 + SEGMENT_HEADER_SIZE);
        EXPECT_EQ(0, std::memcmp(datav[7].data(),
                                 result.result_buf.data() + SEGMENT_HEADER_SIZE,
                                 4 * sizeof(uint8_t)));

        result = segment.read(4, 256);
        ASSERT_EQ(result.result_buf.size(), 4 * (4 + SEGMENT_HEADER_SIZE));
        for (int i = 4; i < 8; ++i) {
            EXPECT_EQ(
                0, std::memcmp(datav[i].data(),
                               result.result_buf.data() +
                                   (4 * sizeof(uint8_t) + SEGMENT_HEADER_SIZE) *
                                       (i - 4) +
                                   SEGMENT_HEADER_SIZE,
                               4 * sizeof(uint8_t)));
        }

        result = segment.read(8, 256);
        EXPECT_TRUE(result.result_buf.empty());
    }

    Segment segment(dir, 0, 7, 32, SegmentState::Sealed);
    EXPECT_TRUE(segment.isFull());
    EXPECT_EQ(segment.getPublishedOffset(), 7);
    EXPECT_TRUE(segment.getPublishedSize() > 0);
    result = segment.read(7, 256);
    ASSERT_EQ(result.result_buf.size(), 4 + SEGMENT_HEADER_SIZE);
    EXPECT_EQ(0, std::memcmp(datav[7].data(),
                             result.result_buf.data() + SEGMENT_HEADER_SIZE,
                             4 * sizeof(uint8_t)));

    result = segment.read(4, 256);
    ASSERT_EQ(result.result_buf.size(), 4 * (4 + SEGMENT_HEADER_SIZE));
    for (int i = 4; i < 8; ++i) {
        EXPECT_EQ(0,
                  std::memcmp(datav[i].data(),
                              result.result_buf.data() +
                                  (4 * sizeof(uint8_t) + SEGMENT_HEADER_SIZE) *
                                      (i - 4) +
                                  SEGMENT_HEADER_SIZE,
                              4 * sizeof(uint8_t)));
    }

    result = segment.read(8, 256);
    EXPECT_TRUE(result.result_buf.empty());
}

TEST_F(StorageEngineTests, IsFull) {
    std::filesystem::path dir = getDir() / "IsFull";
    Segment segment(dir, 0, SEGMENT_HEADER_SIZE + 1, SegmentState::Active);
    std::vector<uint8_t> data;
    data.push_back(1);
    uint64_t offset = segment.append(data.data(), sizeof(uint8_t));
    FetchResult result = segment.read(0, 100);
    ASSERT_EQ(result.result_buf.size(), SEGMENT_HEADER_SIZE + 1);
    EXPECT_TRUE(segment.isFull());
    EXPECT_EQ(data[0], result.result_buf[SEGMENT_HEADER_SIZE]);
}

/*
    TODO: Right now the tests below only read the data from a single append
    But should read from multiple attempts, especially make sure to read across segment boundaries
*/

TEST_F(StorageEngineTests, LogReadWrite) {
    std::filesystem::path dir = getDir() / "LogReadWrite";
    Log log(dir, SEGMENT_HEADER_SIZE + 1);

    std::vector<uint64_t> offsets;
    AppendData data;
    data.data.push_back(2);
    auto offset = log.append(data);
    FetchRequest request;
    request.offset = offset;
    request.max_bytes = 2 * (SEGMENT_HEADER_SIZE + 1);
    auto result = log.fetch(request);
    ASSERT_EQ(result.result_buf.size(), SEGMENT_HEADER_SIZE + 1);
    ASSERT_EQ(result.result_buf[SEGMENT_HEADER_SIZE], 2);
}

TEST_F(StorageEngineTests, LogReadWriteRollover) {
    std::filesystem::path dir = getDir() / "LogReadWriteRollover";
    Log log(dir, 4 * (SEGMENT_HEADER_SIZE + 1));

    std::vector<uint64_t> offsets;
    AppendData data;
    int i;
    for (i = 0; i < 98; ++i) {
        data.data.clear();
        data.data.push_back(i);
        auto offset = log.append(data);
        offsets.push_back(offset);
        ASSERT_EQ(offset, i);
    }
    
    FetchRequest request;
    for (i = 0; i < 98; ++i) {
        request.offset = i;
        request.max_bytes = 100 * (SEGMENT_HEADER_SIZE + 1);
        auto result = log.fetch(request);
        ASSERT_EQ(result.result_buf.size(), (98-i)*(SEGMENT_HEADER_SIZE+1));//100 - i +1?
        for (int j = 0; j < 98-i; ++j) {
            ASSERT_EQ(result.result_buf[(j+1)*(SEGMENT_HEADER_SIZE+1)-1], i+j);
        }
    }
}
/*
    Index tests
    1. sparse and non-sparse
    2. Active and sealed
    3. Adding IndexEntry is monotonic w.r.t. offset, i.e. offset of new entry
       should be strictly larger than current largest indexed offset (Also
       prevents duplicate indexing)
    4. Empty index?
    5. Large index/offset?
*/

TEST_F(StorageEngineTests, IndexRW) {
    std::filesystem::path dir = getDir() / "IndexRW";
    {
        Index index(dir, 0, SegmentState::Active);
        IndexFileEntry entry;
        uint32_t file_pos = 0;
        for (uint64_t i = 0; i < 1000; ++i) {
            entry.offset = i;
            entry.file_position = file_pos;
            index.append(entry);
            file_pos += 25 * (i % 4 + 1);
        }
        file_pos = 0;
        for (uint64_t i = 0; i < 1000; ++i) {
            entry = index.determineClosestIndex(i);
            EXPECT_EQ(entry.offset, i);
            // EXPECT_EQ(entry.file_position, file_pos);
            file_pos += 25 * (i % 4 + 1);
        }
    }
    Index index(dir, 0, SegmentState::Sealed);
    IndexFileEntry entry;
    uint32_t file_pos = 0;
    for (uint64_t i = 0; i < 10; ++i) {
        entry = index.determineClosestIndex(i);
        EXPECT_EQ(entry.offset, i);
        EXPECT_EQ(entry.file_position, file_pos);
        file_pos += 25 * (i % 4 + 1);
    }
}

TEST_F(StorageEngineTests, IndexRWSparse) {
    std::filesystem::path dir = getDir() / "IndexRWSparse";
    {
        Index index(dir, 0, SegmentState::Active);
        IndexFileEntry entry;
        uint32_t file_pos = 0;
        for (uint64_t i = 2; i < 1000; ++i) {
            entry.offset = i * i;
            entry.file_position = file_pos;
            index.append(entry);
            file_pos += 25 * (i % 4 + 1);
        }
        file_pos = 0;
        for (uint64_t i = 2; i < 1000; ++i) {
            entry = index.determineClosestIndex(i * i + i);
            EXPECT_EQ(entry.offset, i * i);
            EXPECT_EQ(entry.file_position, file_pos);
            file_pos += 25 * (i % 4 + 1);
        }
    }
    Index index(dir, 0, SegmentState::Sealed);
    IndexFileEntry entry;
    uint32_t file_pos = 0;
    for (uint64_t i = 2; i < 1000; ++i) {
        entry = index.determineClosestIndex(i * i + i);
        EXPECT_EQ(entry.offset, i * i);
        EXPECT_EQ(entry.file_position, file_pos);
        file_pos += 25 * (i % 4 + 1);
    }
}

TEST_F(StorageEngineTests, IndexMonotonic) {
    std::filesystem::path dir = getDir() / "IndexMonotonic";
    {
        Index index(dir, 0, SegmentState::Active);
        IndexFileEntry entry;
        uint32_t file_pos = 0;
        for (uint64_t i = 0; i < 10; ++i) {
            entry.offset = i;
            entry.file_position = file_pos;
            index.append(entry);
            file_pos += 25 * (i % 4 + 1);
        }
        entry.offset = 5;
        entry.file_position = 25;
        EXPECT_ANY_THROW(index.append(entry));
    }
    // Sealed index does not allow append
    Index index(dir, 0, SegmentState::Sealed);
    IndexFileEntry entry{11, 1500};
    EXPECT_ANY_THROW(index.append(entry));
}

} // namespace broker
} // namespace kafka_lite
