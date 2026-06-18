#include "../include/BrokerClient.h"
#include "../include/BrokerServer.h"
#include "../include/ByteSwap.h"
#include "../include/FakeBrokerCore.h"
#include "../include/RecordProducer.h"
#include <boost/uuid/random_generator.hpp>
#include <cstdint>
#include <cstring>
#include <gtest/gtest.h>
#include <memory>
#include <span>
#include <thread>
#include <vector>

namespace kafka_lite {
namespace broker {

class TestServer {
  public:
    TestServer()
        : server_(49153, std ::make_unique<FakeBrokerCore>(), io_context_) {}
    void start() {
        io_context_thread = std::thread([this]() { io_context_.run(); });
    };
    void stop() { io_context_.stop(); io_context_thread.join(); }
    unsigned int port() { return server_.port(); }

  private:
    boost::asio::io_context io_context_;
    BrokerServer server_;
    std::thread io_context_thread;
};

class BrokerServerTests : public ::testing::Test {
  private:
  protected:
    void SetUp() override { server_.start(); }
    void TearDown() override { server_.stop(); }
    TestServer server_;
};

TEST_F(BrokerServerTests, append_ok) {
    BrokerClient client(server_.port());
    std::vector<uint8_t> payload{1,2,3,4};
    auto response = client.append(payload);
    ASSERT_EQ(response.response_code, 0);
    ASSERT_TRUE(response.payload.has_value());
    EXPECT_EQ(response.payload->size(), sizeof(uint64_t));
    uint64_t offset = -1;
    std::memcpy(&offset, response.payload->data(), sizeof(offset));
    if (byteswap::is_big_endian())
        offset = byteswap::byteswap64(offset);
    EXPECT_EQ(offset, 0);
}

TEST_F(BrokerServerTests, send_raw_append_request_ok) {
    BrokerClient client(server_.port());
    std::vector<uint8_t> payload{1, 2, 3, 4};
    payload = RecordProducer::create_record(payload);
    boost::uuids::random_generator generator;
    auto correlation_id = generator();
    TcpHeaders headers(correlation_id, 0, RequestType::Append, 0);
    TcpRequest request{.headers = headers, .payload = payload};
    auto response = client.send_raw_request(request);
    ASSERT_EQ(response.response_code, 0);
    ASSERT_TRUE(response.payload.has_value());
    EXPECT_EQ(response.payload->size(), sizeof(uint64_t));
    EXPECT_EQ(response.correlation_id, correlation_id);
    uint64_t offset = -1;
    std::memcpy(&offset, response.payload->data(), sizeof(offset));
    if (byteswap::is_big_endian())
        offset = byteswap::byteswap64(offset);
    EXPECT_EQ(offset, 0);
}

TEST_F(BrokerServerTests, append_unsupported_version) {
    BrokerClient client(server_.port());
    std::vector<uint8_t> payload{1, 2, 3, 4};
    payload = RecordProducer::create_record(payload);
    boost::uuids::random_generator generator;
    auto correlation_id = generator();
    TcpHeaders headers(correlation_id, 1, RequestType::Append, 0);
    TcpRequest request{.headers = headers, .payload = payload};
    auto response = client.send_raw_request(request);
    ASSERT_EQ(response.response_code, 2);
    EXPECT_EQ(response.correlation_id, correlation_id);
    ASSERT_FALSE(response.payload.has_value());
}

TEST_F(BrokerServerTests, append_unsupported_flags) {
    BrokerClient client(server_.port());
    std::vector<uint8_t> payload{1, 2, 3, 4};
    payload = RecordProducer::create_record(payload);
    boost::uuids::random_generator generator;
    auto correlation_id = generator();
    TcpHeaders headers(correlation_id, 0, RequestType::Append, 1);
    TcpRequest request{.headers = headers, .payload = payload};
    auto response = client.send_raw_request(request);
    ASSERT_EQ(response.response_code, 4);
    EXPECT_EQ(response.correlation_id, correlation_id);
    ASSERT_FALSE(response.payload.has_value());
}

TEST_F(BrokerServerTests, append_fetch_one) {
    BrokerClient client(server_.port());
    std::vector<uint8_t> payload{1, 2, 3, 4};
    auto append_response = client.append(payload);
    ASSERT_EQ(append_response.response_code, 0);
    ASSERT_TRUE(append_response.payload.has_value());
    EXPECT_EQ(append_response.payload->size(), sizeof(uint64_t));
    uint64_t offset = -1;
    std::memcpy(&offset, append_response.payload->data(), sizeof(offset));
    if (byteswap::is_big_endian())
        offset = byteswap::byteswap64(offset);
    EXPECT_EQ(offset, 0);

    auto fetch_response = client.fetch(offset, 1024);
    ASSERT_EQ(fetch_response.response_code, 0);
    ASSERT_TRUE(fetch_response.payload.has_value());
    ASSERT_EQ(fetch_response.payload->size(), payload.size() + 16);
    std::vector<uint8_t> fetch_payload(payload.size());
    fetch_payload.assign(fetch_response.payload->begin()+16, fetch_response.payload->end());
    ASSERT_EQ(fetch_payload, payload);
}

// server should reject if checksum is wrong
TEST_F(BrokerServerTests, append_wrong_checksum) { BrokerClient client(server_.port()); }

} // namespace broker
} // namespace kafka_lite
