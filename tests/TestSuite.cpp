#include "../include/Segment.h"
#include "TempFile.h"
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

class BrokerLibTests : public ::testing::Test {
  private:
    std::filesystem::path dir_;

  public:
    void setDir(const std::filesystem::path &dir) {dir_=dir;}
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
    Segment segment(dir, 0, 32,
                    SegmentState::Active);
    std::vector<uint8_t> data;
    data.reserve(4);
    for (int i = 1; i < 5; ++i) {
        data.push_back(i);
    }
    uint64_t offset =
        segment.append(data.data(), data.size() * sizeof(uint8_t));
    FetchResult result = segment.read(offset, 32);
    ASSERT_EQ(result.offset, offset);
    ASSERT_EQ(result.result_buf, data);// This is not the best test, since segment also writes in the length
};
