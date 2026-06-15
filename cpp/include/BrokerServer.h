#ifndef BROKER_SERVER_HH
#define BROKER_SERVER_HH

#include "BrokerCoreIfc.h"
#include "TcpProtocol.h"
#include <array>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <queue>
#include <variant>
#include <vector>

using tcp = boost::asio::ip::tcp;

namespace kafka_lite {
namespace broker {

enum class BrokerServerStatus { Starting, Active, Stopping, Stopped };

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
  public:
    tcp::socket &socket() { return socket_; }
    void start();

    static std::shared_ptr<TcpConnection>
    create(boost::asio::io_context &io_context,
           std::unique_ptr<BrokerCoreIfc> &core);
    static std::variant<AppendRequest, FetchRequest>
    parseTcpRequest(const TcpHeaders &headers,
                    const std::vector<uint8_t> &payload_bytes);

  private:
    TcpConnection(boost::asio::io_context &io_context,
                  std::unique_ptr<BrokerCoreIfc> &core);
    void stop();
    void doReadHeaderLength();
    void doReadMagicBytes();
    void doReadPayloadLength();
    void doReadHeaders(uint32_t length);
    void doReadPayload(uint32_t length);
    void handleTcpRequest(std::vector<uint8_t> header_bytes,
                          std::vector<uint8_t> payload_bytes);
    void handleAppendRequest(const AppendRequest &request);
    void handleFetchRequest(const FetchRequest &request);
    void sendResponse(const TcpResponse &response);
    void doWrite();
    void handleWrite(const boost::system::error_code &ec, size_t bytes_written);

    tcp::socket socket_;
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    std::queue<TcpResponse> write_queue_;
    std::array<uint8_t, 4> length_buf_;
    std::array<uint8_t, 5> magic_bytes_buf_;
    std::vector<uint8_t> header_read_buf_;
    std::vector<uint8_t> payload_read_buf_;
    std::unique_ptr<BrokerCoreIfc> &core_;
    bool write_in_progress_, stopped_;
};

class BrokerServer : public std::enable_shared_from_this<BrokerServer> {
  public:
    BrokerServer(std::unique_ptr<BrokerCoreIfc> core,
                 boost::asio::io_context &io_context);
  private:
    void startAccept();
    void handleAccept(std::shared_ptr<TcpConnection> connection,
                      const boost::system::error_code &ec);
    std::unique_ptr<BrokerCoreIfc> core_;
    boost::asio::io_context &iocontext_;
    tcp::acceptor tcp_acceptor_;
    BrokerServerStatus status_;
};
} // namespace broker
} // namespace kafka_lite
#endif
