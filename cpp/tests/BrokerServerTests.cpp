#include "../include/BrokerServer.h"
#include "../include/FakeBrokerCore.h"
#include <gtest/gtest.h>
#include <memory>
#include <thread>

namespace kafka_lite {
namespace broker {

class TestServer {
  public:
    TestServer(): server_(std::make_unique<FakeBrokerCore>(), io_context_) {}
    void start() {
        io_context_thread = std::thread([this](){io_context_.run();});
    };
    void stop() {io_context_.stop();}

  private:
    boost::asio::io_context io_context_;
    BrokerServer server_;
    std::thread io_context_thread;

};

class BrokerServerTests : public ::testing::Test {
  private:

  protected:
    void SetUp() override { server_.start();}
    void TearDown() override { server_.stop(); }
    TestServer server_;
};

} // namespace broker
} // namespace kafka_lite
