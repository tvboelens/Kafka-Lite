#include "../include/Segment.h"
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

class BrokerLibTests : public ::testing::Test {
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

void BrokerLibTests::TearDown() { std::filesystem::remove_all(dir_); }

TEST_F(BrokerLibTests, SegmentRW) {
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
    ASSERT_EQ(result.offset, offset);
    ASSERT_EQ(result_buf, data);
    ASSERT_EQ(segment.getBaseOffset(), segment.getPublishedOffset());
};

/*
        1. Index also has to be tested: Here we would have two different
           situations for sealed and active segments
        2. How to deal with sparsity in the indexing? Right now I index everything, but what about later?
*/

TEST_F(BrokerLibTests, SegmentRWMultiple) {
    std::filesystem::path dir = getDir() / "SegmentRWMultiple";
    Segment segment(dir, 0, 32, SegmentState::Active);
    std::vector<uint8_t> data;
    std::vector<std::vector<uint8_t>> datav;
    datav.reserve(8);
    data.resize(4);
    std::vector<uint64_t> offsets;
    offsets.resize(8);
    data.reserve(4);
    for (int i = 0; i < 8; ++i) {
        for (int j = 0; j < 4; ++j) {
            data[j] = i * 4 + j;
        }
        datav.push_back(data);
        offsets[i] = segment.append(data.data(), data.size() * sizeof(uint8_t));
    }
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(offsets[i], i);
    }
    EXPECT_TRUE(segment.isFull());
    FetchResult result = segment.read(7, 256);
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

TEST_F(BrokerLibTests, IsFull) {
    std::filesystem::path dir = getDir() / "IsFull";
    Segment segment(dir, 0, SEGMENT_HEADER_SIZE + 1, SegmentState::Active);
    std::vector<uint8_t> data;
    data.push_back(1);
    uint64_t offset = segment.append(data.data(), sizeof(uint8_t));
    FetchResult result = segment.read(0, 100);
    ASSERT_EQ(result.result_buf.size(), SEGMENT_HEADER_SIZE+1);
    EXPECT_TRUE(segment.isFull());
    EXPECT_EQ(data[0], result.result_buf[SEGMENT_HEADER_SIZE]);
}
