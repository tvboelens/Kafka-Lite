#include "../include/BrokerServer.h"
#include "../include/ByteSwap.h"
#include "../include/TcpProtocol.h"
#include <array>
#include <boost/asio.hpp>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <system_error>
#include <variant>
#include <vector>

namespace kafka_lite {
namespace broker {

using namespace kafka_lite::byteswap;

uint32_t parseLength(std::array<uint8_t, 4> len_header_buf) {
    uint32_t len;
    std::memcpy(&len, len_header_buf.data(), sizeof(len));
    if (!byteswap::is_big_endian())
        len = byteswap32(len);
    return len;
}

std::variant<AppendRequest, FetchRequest>
TcpConnection::parseTcpRequest(const TcpHeaders &headers,
                               const std::vector<uint8_t> &payload_bytes) {
    TcpRequest request{headers, payload_bytes};
    return request.to_specialized_type();
}

std::shared_ptr<TcpConnection>
TcpConnection::create(boost::asio::io_context &io_context,
                      std::unique_ptr<BrokerCoreIfc> &core) {
    return std::shared_ptr<TcpConnection>(new TcpConnection(io_context, core));
}

TcpConnection::TcpConnection(boost::asio::io_context &io_context,
                             std::unique_ptr<BrokerCoreIfc> &core)
    : strand_(boost::asio::make_strand(io_context)), socket_(io_context),
      core_(core) {}

void TcpConnection::start() {
    boost::asio::post(
        strand_, [self = shared_from_this()]() { self->doReadHeaderLength(); });
}

void TcpConnection::doReadHeaderLength() {
    boost::asio::async_read(
        socket_, boost::asio::buffer(length_buf_),
        [self = shared_from_this()](boost::system::error_code ec,
                                    size_t bytes_read) {
            if (ec) {
                self->stop();
                return;
            }
            self->doReadMagicBytes();
        });
}

void TcpConnection::doReadMagicBytes() {
    boost::asio::async_read(
        socket_, boost::asio::buffer(magic_bytes_buf_),
        [self = shared_from_this()](boost::system::error_code ec,
                                    size_t bytes_read) {
            if (ec ||
                self->magic_bytes_buf_ !=
                    std::array<uint8_t, 5>({0x6B, 0x61, 0x66, 0x6B, 0x61})) {
                self->stop();
                return;
            }
            uint32_t length = parseLength(self->length_buf_);
            self->doReadHeaders(length);
        });
}

void TcpConnection::doReadPayloadLength() {
    boost::asio::async_read(
        socket_, boost::asio::buffer(length_buf_),
        [self = shared_from_this()](boost::system::error_code ec,
                                    size_t bytes_read) {
            if (ec) {
                self->stop();
                return;
            }
            uint32_t length = parseLength(self->length_buf_);
            self->doReadPayload(length);
        });
}

void TcpConnection::doReadHeaders(uint32_t length) {
    header_read_buf_.resize(length);
    boost::asio::async_read(
        socket_, boost::asio::buffer(header_read_buf_),
        [self = shared_from_this()](boost::system::error_code ec,
                                    size_t bytes_read) {
            if (ec) {
                self->stop();
                return;
            }
            self->doReadPayloadLength();
        });
}

void TcpConnection::doReadPayload(uint32_t length) {
    payload_read_buf_.resize(length);
    boost::asio::async_read(
        socket_, boost::asio::buffer(payload_read_buf_),
        [self = shared_from_this()](boost::system::error_code ec,
                                    size_t bytes_read) {
            if (ec) {
                self->stop();
                return;
            }
            self->handleTcpRequest(std::move(self->header_read_buf_),
                                   std::move(self->payload_read_buf_));
        });
}

void TcpConnection::handleTcpRequest(std::vector<uint8_t> header_bytes,
                                     std::vector<uint8_t> payload_bytes) {
    TcpHeaders headers;
    if (!headers.from_bytes(header_bytes)) {
        auto response = TcpResponse::makeErrorResponse(headers.correlation_id,
                                                         headers.getParseError());
        sendResponse(response);
        return;
    }
    auto request = parseTcpRequest(headers, payload_bytes);
    if (std::holds_alternative<AppendRequest>(request)) {
        handleAppendRequest(std::get<AppendRequest>(request));
    } else {
        handleFetchRequest(std::get<FetchRequest>(request));
    }
}

void TcpConnection::handleAppendRequest(const AppendRequest &request) {
    AppendData data{request.payload};
    core_->submit_append(
        data, [self = shared_from_this(), cor_id = request.correlation_id](uint64_t offset, std::error_code ec) {
            boost::asio::post(self->strand_, [self, cor_id, offset, ec]() {
                TcpResponse response = TcpResponse::makeResponse(cor_id, offset, ec);
                self->sendResponse(response);
            });
        });
}

void TcpConnection::handleFetchRequest(const FetchRequest &request) {
    // TODO: handle max_bytes too large
    FetchData data{.offset = request.offset, .max_bytes = request.max_bytes};
    core_->submit_fetch(
        data, [self = shared_from_this(), cor_id = request.correlation_id](const FetchResult &result,
                                             std::error_code ec) {
            boost::asio::post(self->strand_, [self, cor_id, result, ec]() {
                TcpResponse response = TcpResponse::makeResponse(cor_id, result, ec);
                self->sendResponse(response);
            });
        });
}

void TcpConnection::sendResponse(const TcpResponse &response) {
    write_queue_.push(response);
    if (!write_in_progress_)
        doWrite();
}

void TcpConnection::doWrite() {
    write_in_progress_ = true;
    boost::asio::async_write(
        socket_, boost::asio::buffer(write_queue_.front().to_bytes()),
        boost::asio::bind_executor(
            strand_, [self = shared_from_this()](boost::system::error_code ec,
                                                 size_t bytes_written) {
                self->handleWrite(ec, bytes_written);
            }));
}

void TcpConnection::handleWrite(const boost::system::error_code &ec,
                                size_t bytes_written) {
    if (ec) {
        stop();
        return;
    }
    write_queue_.pop();
    if (!write_queue_.empty()) {
        doWrite();
    } else {
        write_in_progress_ = false;
    }
}

void TcpConnection::stop() {
    if (stopped_)
        return;
    stopped_ = true;
    boost::system::error_code ec;
    auto rc = socket_.shutdown(tcp::socket::shutdown_receive, ec);
    rc = socket_.shutdown(tcp::socket::shutdown_send, ec);
    rc = socket_.close(ec);
}

BrokerServer::BrokerServer(unsigned int port, std::unique_ptr<BrokerCoreIfc> core,
                           boost::asio::io_context &io_context)
    : port_(port), status_(BrokerServerStatus::Starting), core_(std::move(core)),
      iocontext_(io_context),
      tcp_acceptor_(iocontext_, tcp::endpoint(tcp::v4(), port_)) {
    core_->start();
    status_ = BrokerServerStatus::Active;
    startAccept();
}

void BrokerServer::startAccept() {
    std::shared_ptr<TcpConnection> connection =
        TcpConnection::create(iocontext_, core_);
    tcp_acceptor_.async_accept(connection->socket(),
                               std::bind(&BrokerServer::handleAccept, this,
                                         connection,
                                         boost::asio::placeholders::error));
}

void BrokerServer::handleAccept(std::shared_ptr<TcpConnection> connection,
                                const boost::system::error_code &ec) {
    if (!ec) {
        connection->start();
    }
    startAccept();
}

} // namespace broker
} // namespace kafka_lite
