#ifndef BROKERCLIENT_HH
#define BROKERCLIENT_HH

#include "TcpProtocol.h"
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <cstdint>
#include <vector>

namespace kafka_lite {
namespace broker {

using tcp = boost::asio::ip::tcp;

class BrokerClient {
  public:
    BrokerClient(unsigned int port);
    TcpResponse append(const std::vector<uint8_t> &payload);
    TcpResponse fetch(uint64_t offset, uint32_t max_bytes);
    TcpResponse send_raw_request(const TcpRequest &request); // for testing

  private:
    void send_header_len_and_magic_bytes(uint32_t len, tcp::socket &socket);
    void send_payload(tcp::socket &socket, const std::vector<uint8_t> &payload);
    TcpResponse recv_response(tcp::socket &socket);
    unsigned int port_;
    boost::asio::io_context io_context_;
};

} // namespace broker
} // namespace kafka_lite

#endif
