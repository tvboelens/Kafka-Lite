#include "../include/BrokerServer.h"

namespace kafka_lite {
namespace broker {
BrokerServer::BrokerServer(std::unique_ptr<BrokerCore> &core,
                           boost::asio::io_context &io_context)
    : status_(BrokerServerStatus::Starting), core_(std::move(core)),
      iocontext_(io_context),
      tcp_acceptor_(iocontext_, tcp::endpoint(tcp::v6(), 13)) {
    core->start();
    status_ = BrokerServerStatus::Active;
}
} // namespace broker
} // namespace kafka_lite
