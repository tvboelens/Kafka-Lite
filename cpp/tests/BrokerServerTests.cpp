#include "../include/BrokerClient.h"
#include "../include/BrokerServer.h"
#include "../include/FakeBrokerCore.h"
#include <gtest/gtest.h>
#include <memory>
#include <thread>

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

TEST_F(BrokerServerTests, append_ok) { BrokerClient client(server_.port()); }

// server should reject if checksum is wrong
TEST_F(BrokerServerTests, append_wrong_checksum) { BrokerClient client(server_.port()); }

} // namespace broker
} // namespace kafka_lite
