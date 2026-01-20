#include "../include/Segment.h"
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

class BrokerLibTests : public ::testing::Test {
  private:
    std::filesystem::path dir_;

  public:
    void setDir(const std::filesystem::path &dir) {dir_ = dir;}
  protected:
    void SetUp() override {};
    void TearDown() override;
};

void BrokerLibTests::TearDown() {
    std::filesystem::remove_all(dir_);
}

TEST_F(BrokerLibTests, SegmentRW) {
    std::filesystem::path dir = std::filesystem::current_path() / "Segment";
    setDir(dir);
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
};
