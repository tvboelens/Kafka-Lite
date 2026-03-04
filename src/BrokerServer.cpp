#include "../include/BrokerServer.h"
#include <algorithm>
#include <array>
#include <boost/asio.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <system_error>
#include <variant>
#include <vector>

namespace kafka_lite {
namespace broker {

TcpResponse makeResponse(uint64_t offset, const std::error_code &ec) {
    return {};
}
TcpResponse makeResponse(const FetchResult &result, const std::error_code &ec) {
    return {};
}

uint32_t parseLength(std::array<uint8_t, 4> len_header_buf) { return 0; }

std::variant<AppendRequest, FetchRequest>
parseTcpRequest(const std::vector<uint8_t> &bytes) {}

std::shared_ptr<TcpConnection>
TcpConnection::create(boost::asio::io_context &io_context,
                      std::unique_ptr<BrokerCore> &core) {
    return std::shared_ptr<TcpConnection>(new TcpConnection(io_context, core));
}

TcpConnection::TcpConnection(boost::asio::io_context &io_context,
                             std::unique_ptr<BrokerCore> &core)
    : strand_(boost::asio::make_strand(io_context)), socket_(io_context),
      core_(core) {}

void TcpConnection::start() { doReadLengthHeader(); }

void TcpConnection::doReadLengthHeader() {
    boost::asio::async_read(
        socket_, boost::asio::buffer(length_header_buf_),
        [this](boost::system::error_code ec, size_t bytes_read) {
            uint32_t length = parseLength(length_header_buf_);
            doReadTcpRequest(length);
        });
}

void TcpConnection::doReadTcpRequest(uint32_t length) {
    read_buf_.resize(length);
    boost::asio::async_read(
        socket_, boost::asio::buffer(read_buf_),
        [this, self = shared_from_this()](boost::system::error_code ec,
                                          size_t bytes_read) {
            if (ec) {
                stop();
                return;
            }
            std::vector<uint8_t> bytes(read_buf_);
            handleTcpRequest(std::move(bytes));
            doReadLengthHeader();
        });
}

void TcpConnection::handleTcpRequest(std::vector<uint8_t> bytes) {
    auto request = parseTcpRequest(bytes);
    if (std::holds_alternative<AppendRequest>(request)) {
        handleAppendRequest(std::get<AppendRequest>(request));
    } else {
        handleFetchRequest(std::get<FetchRequest>(request));
    }
}

void TcpConnection::handleAppendRequest(const AppendRequest &request) {
    AppendData data{request.payload};
    core_->submit_append(
        data, [self = shared_from_this()](uint64_t offset, std::error_code ec) {
            boost::asio::post(self->strand_, [self, offset, ec]() {
                TcpResponse response = makeResponse(offset, ec);
                self->sendResponse(response);
            });
        });
}

void TcpConnection::handleFetchRequest(const FetchRequest &request) {
    core_->submit_fetch(
        request, [self = shared_from_this()](const FetchResult &result,
                                             std::error_code ec) {
            boost::asio::post(self->strand_, [self, result, ec]() {
                TcpResponse response = makeResponse(result, ec);
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
        socket_, boost::asio::buffer(write_queue_.front().bytes),
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
    if (!rc) {
        std::string msg =
            "Error when shutting down socket, error code: " + ec.to_string();
        throw std::runtime_error(msg);
    }
    while (!write_queue_.empty());
    rc = socket_.shutdown(tcp::socket::shutdown_send, ec);
    if (!rc) {
        std::string msg =
            "Error when shutting down socket, error code: " + ec.to_string();
        throw std::runtime_error(msg);
    }
    rc = socket_.close(ec);
    if (!rc) {
        std::string msg =
            "Error when closing socket, error code: " + ec.to_string();
        throw std::runtime_error(msg);
    }
}

BrokerServer::BrokerServer(std::unique_ptr<BrokerCore> &core,
                           boost::asio::io_context &io_context)
    : status_(BrokerServerStatus::Starting), core_(std::move(core)),
      iocontext_(io_context),
      tcp_acceptor_(iocontext_, tcp::endpoint(tcp::v6(), 13)) {
    core->start();
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
