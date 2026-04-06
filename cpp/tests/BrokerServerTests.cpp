#include "../include/BrokerServer.h"
#include <cstdint>
#include <gtest/gtest.h>
#include <variant>
#include <vector>

namespace kafka_lite {
namespace broker {

class BrokerServerTests : public ::testing::Test {
  private:
    std::filesystem::path dir_;

  public:
    std::filesystem::path getDir() { return dir_; }

  protected:
    void SetUp() override { dir_ = std::filesystem::current_path() / "Server"; }
    void TearDown() override { std::filesystem::remove_all(dir_); }
};

/*
1. succesful append request
2. succesful fetch request
3. Checksum mismatch
4. Type header leads to right kind of request
5. Endianness?
6. payload is parsed correctly -> function to serialize a TCP request?
*/

/* TEST(BrokerServerTests, parseAppendRequest) {
    std::vector<uint8_t> bytes;
    auto request = TcpConnection::parseTcpRequest(bytes);
    EXPECT_TRUE(std::holds_alternative<AppendRequest>(request));
} */

} // namespace broker
} // namespace kafka_lite
