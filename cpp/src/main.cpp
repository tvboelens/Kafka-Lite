#include "../include/BrokerCore.h"
#include "../include/BrokerServer.h"
#include "boost/asio/io_context.hpp"
#include "boost/asio/signal_set.hpp"
#include <csignal>
#include <filesystem>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

using BrokerCoreIfc = kafka_lite::broker::BrokerCoreIfc;
using BrokerCore = kafka_lite::broker::BrokerCore;

void print_exit_message(int sig) {
    if (sig == SIGINT)
        std::cout << "received signal SIGINT, shutting down..." << std::endl;
    if (sig == SIGTERM)
        std::cout << "received signal SIGTERM, shutting down..." << std::endl;
}

int main() {
    // todo: make this configurable as well as no of threads
    auto dir = std::filesystem::current_path() / "BrokerDir";
    std::unique_ptr<BrokerCoreIfc> core =
        std::make_unique<BrokerCore>(dir, 16 * 1024);
    unsigned int port = 49153;
    boost::asio::io_context io_context;
    kafka_lite::broker::BrokerServer server(port, std ::move(core), io_context);
    boost::asio::signal_set signals(io_context, SIGINT, SIGTERM);
    signals.async_wait([&](const boost::system::error_code &, int sig) {
        print_exit_message(sig);
        io_context.stop();
    });
    std::vector<std::thread> threads;
    for (unsigned int i = 0; i < 4; ++i) {
        threads.push_back(std::thread([&]() { io_context.run(); }));
    }
    io_context.run();
    for (auto &thread : threads) {
        if (thread.joinable())
            thread.join();
    }
    std::cout << "shutdown complete" << std::endl;
    return 0;
}
