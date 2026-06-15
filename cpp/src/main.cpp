#include "../include/BrokerCore.h"
#include "../include/BrokerServer.h"
#include "boost/asio/io_context.hpp"
#include <filesystem>
#include <memory>

using BrokerCoreIfc = kafka_lite::broker::BrokerCoreIfc;
using BrokerCore = kafka_lite::broker::BrokerCore;
int main() {
    auto dir = std::filesystem::current_path() / "BrokerDir";
    std::unique_ptr<BrokerCoreIfc> core =
        std::make_unique<BrokerCore>(dir, 1024);
    boost::asio::io_context io_context;
    kafka_lite::broker::BrokerServer server(std::move(core), io_context);
    io_context.run();
    return 0;
}
