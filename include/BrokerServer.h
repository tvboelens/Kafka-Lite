#ifndef BROKER_SERVER_HH
#define BROKER_SERVER_HH

#include "BrokerCore.h"
#include "boost/asio/io_context.hpp"
#include "boost/asio/ip/tcp.hpp"
#include <memory>

using tcp = boost::asio::ip::tcp;

namespace kafka_lite {
namespace broker {
class BrokerServer {
  public:
    BrokerServer(std::unique_ptr<BrokerCore> &core,
                 boost::asio::io_context &io_context)
        : core_(std::move(core)), iocontext_(io_context),
          tcp_acceptor_(iocontext_, tcp::endpoint(tcp::v6(), 13)) {}
    // TODO: port number for TCP, 13 is for daytime server
  private:
    std::unique_ptr<BrokerCore> core_;
    boost::asio::io_context &iocontext_;
    tcp::acceptor tcp_acceptor_;
};
} // namespace broker
} // namespace kafka_lite
#endif
