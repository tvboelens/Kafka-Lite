#include "../include/BrokerClient.h"
#include "../include/ByteSwap.h"
#include "../include/RecordManager.h"
#include <array>
#include <boost/asio.hpp>
#include <boost/uuid/random_generator.hpp>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace kafka_lite {
namespace broker {

using namespace boost::uuids;
using namespace kafka_lite::byteswap;
using tcp = boost::asio::ip::tcp;

BrokerClient::BrokerClient(unsigned int port) : port_(port) {}

TcpResponse BrokerClient::append(const std::vector<uint8_t> &payload) {
    random_generator generator;
    auto correlation_id = generator();
    TcpHeaders headers(correlation_id, 0, RequestType::Append, 0);
    auto record = RecordManager::create_record(payload);
    auto header_bytes = headers.to_bytes();
    tcp::socket socket(io_context_);
    tcp::resolver resolver(io_context_);
    tcp::resolver::results_type endpoints =
        resolver.resolve("localhost", std::to_string(port_));
    boost::asio::connect(socket, endpoints);
    send_header_len_and_magic_bytes(header_bytes.size(), socket);
    boost::asio::write(socket, boost::asio::buffer(header_bytes));
    send_payload(socket, record.to_bytes_with_len());
    return recv_response(socket);
}

TcpResponse BrokerClient::fetch(uint64_t offset, uint32_t max_bytes) {
    random_generator generator;
    auto correlation_id = generator();
    TcpHeaders headers(correlation_id, 0, RequestType::Fetch, 0);
    auto payload = TcpRequest::make_payload(offset, max_bytes);
    auto header_bytes = headers.to_bytes();
    tcp::socket socket(io_context_);
    tcp::resolver resolver(io_context_);
    tcp::resolver::results_type endpoints =
        resolver.resolve("localhost", std::to_string(port_));
    boost::asio::connect(socket, endpoints);
    send_header_len_and_magic_bytes(header_bytes.size(), socket);
    boost::asio::write(socket, boost::asio::buffer(header_bytes));
    send_payload(socket, payload);
    return recv_response(socket);
}

TcpResponse BrokerClient::send_raw_request(const TcpRequest &request) {
    tcp::socket socket(io_context_);
    tcp::resolver resolver(io_context_);
    tcp::resolver::results_type endpoints =
        resolver.resolve("localhost", std::to_string(port_));
    boost::asio::connect(socket, endpoints);
    auto header_bytes = request.headers.to_bytes();
    uint32_t len = header_bytes.size();
    send_header_len_and_magic_bytes(len, socket);
    boost::asio::write(socket, boost::asio::buffer(header_bytes));
    send_payload(socket, request.payload);
    return recv_response(socket);
}

void BrokerClient::send_header_len_and_magic_bytes(uint32_t len,
                                                   tcp::socket &socket) {
    std::array<uint8_t, sizeof(len)> len_buf;
    if (!byteswap::is_big_endian())
        len = byteswap::byteswap32(len);
    std::memcpy(len_buf.data(), &len, sizeof(len));
    boost::asio::write(socket, boost::asio::buffer(len_buf));
    boost::asio::write(socket, boost::asio::buffer(MAGIC_BYTES));
}

void BrokerClient::send_payload(tcp::socket &socket,
                                const std::vector<uint8_t> &payload) {
    uint32_t len = payload.size();
    if (!byteswap::is_big_endian())
        len = byteswap::byteswap32(len);
    std::array<uint8_t, sizeof(len)> len_buf;
    std::memcpy(len_buf.data(), &len, sizeof(len));
    boost::asio::write(socket, boost::asio::buffer(len_buf));
    boost::asio::write(socket, boost::asio::buffer(payload));
}

TcpResponse BrokerClient::recv_response(tcp::socket &socket) {
    uint32_t response_len = 0;
    std::array<uint8_t, sizeof(response_len)> len_buf;
    boost::asio::read(socket, boost::asio::buffer(len_buf));
    std::memcpy(&response_len, len_buf.data(), sizeof(response_len));
    if (!byteswap::is_big_endian())
        response_len = byteswap::byteswap32(response_len);
    std::vector<uint8_t> recv_bytes(response_len);
    boost::asio::read(socket, boost::asio::buffer(recv_bytes));
    std::vector<uint8_t> response_bytes(sizeof(response_len) +
                                        recv_bytes.size());
    if (!byteswap::is_big_endian())
        response_len = byteswap::byteswap32(response_len);
    std::memcpy(response_bytes.data(), &response_len, sizeof(response_len));
    std::memcpy(response_bytes.data() + sizeof(response_len), recv_bytes.data(),
                recv_bytes.size());
    return TcpResponse::from_bytes(response_bytes);
}

} // namespace broker
} // namespace kafka_lite
