#ifndef BROKER_SERVER_HH
#define BROKER_SERVER_HH

#include "BrokerCore.h"
#include "Log.h"
#include "Segment.h"
#include <array>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <queue>
#include <system_error>
#include <variant>
#include <vector>

using tcp = boost::asio::ip::tcp;

namespace kafka_lite {
namespace broker {

enum class BrokerServerStatus { Starting, Active, Stopping, Stopped };

struct AppendRequest {
    std::vector<uint8_t> payload;
};

struct TcpResponse {
    std::vector<uint8_t> bytes;
};

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
  public:
    tcp::socket &socket() { return socket_; }
    void start();

    static std::shared_ptr<TcpConnection>
    create(boost::asio::io_context &io_context,
           std::unique_ptr<BrokerCore> &core);
    static std::variant<AppendRequest, FetchRequest>
    parseTcpRequest(const std::vector<uint8_t> &header_bytes,
                    const std::vector<uint8_t> &payload_bytes);
    static TcpResponse makeResponse(uint64_t offset, const std::error_code &ec);
    static TcpResponse makeResponse(const FetchResult &result,
                                    const std::error_code &ec);

  private:
    TcpConnection(boost::asio::io_context &io_context,
                  std::unique_ptr<BrokerCore> &core);
    void stop();
    void doReadHeaderLength();
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
    std::vector<uint8_t> header_read_buf_;
    std::vector<uint8_t> payload_read_buf_;
    std::unique_ptr<BrokerCore> &core_;
    bool write_in_progress_, stopped_;
};

class BrokerServer : public std::enable_shared_from_this<BrokerServer> {
  public:
    BrokerServer(std::unique_ptr<BrokerCore> &core,
                 boost::asio::io_context &io_context);
    // TODO: port number for TCP, 13 is for daytime server
  private:
    void startAccept();
    void handleAccept(std::shared_ptr<TcpConnection> connection,
                      const boost::system::error_code &ec);
    std::unique_ptr<BrokerCore> core_;
    boost::asio::io_context &iocontext_;
    tcp::acceptor tcp_acceptor_;
    BrokerServerStatus status_;
};
} // namespace broker
} // namespace kafka_lite
#endif
