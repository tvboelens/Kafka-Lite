#include "../include/BrokerServer.h"
#include "boost/asio/io_context.hpp"
#include <filesystem>
#include <memory>

int main() {
    auto dir = std::filesystem::current_path()/"Broker";
    auto core = std::make_unique<kafka_lite::broker::BrokerCore>(dir, 1024);
    boost::asio::io_context io_context;
    kafka_lite::broker::BrokerServer server(core, io_context);
    io_context.run();
    return 0;
}
