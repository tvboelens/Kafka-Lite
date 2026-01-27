#include "../include/Segment.h"
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

class BrokerLibTests : public ::testing::Test {
  private:
    std::filesystem::path dir_;

  public:
    std::filesystem::path getDir() { return dir_; }

  protected:
    void SetUp() override{
        dir_ = std::filesystem::current_path() /
                                    "Segment";} void TearDown() override;
};

void BrokerLibTests::TearDown() {
    std::filesystem::remove_all(dir_);
}

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
              it != result.result_buf.end();
         ++it) {
        result_buf.push_back(*it);
	}
    ASSERT_EQ(result.offset, offset);
    ASSERT_EQ(result_buf, data);
    ASSERT_EQ(segment.getBaseOffset(), segment.getPublishedOffset());
};

TEST_F(BrokerLibTests, SegmentRWMultiple) {
    std::filesystem::path dir = getDir() / "SegmentRWMultiple";
    Segment segment(dir, 0, 32, SegmentState::Active);
    FetchResult result = segment.read(0, 32);
    std::vector<uint8_t> data, result_buf;
    data.resize(4);
    std::vector<uint64_t> offsets;
    offsets.resize(8);
    data.reserve(4);
    for (int i = 0; i < 8; ++i) {
        for (int j = 0; j < 4; ++j) {
            data[j] = i*4 + j;
        }
        offsets[i] = segment.append(data.data(), data.size()*sizeof(uint8_t));
    }
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(offsets[i], i);
    }
    EXPECT_TRUE(segment.isFull());
}
