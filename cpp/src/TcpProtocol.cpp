#include "../include/TcpProtocol.h"
#include "../include/ByteSwap.h"
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace kafka_lite {
namespace broker {

using namespace kafka_lite::byteswap;

TcpHeaders::TcpHeaders(const uuid &correlation_id, uint8_t ptcl_version,
                       RequestType type, uint16_t flags)
    : correlation_id(correlation_id), protocol_version(ptcl_version),
      type(type), flags(flags), parse_error(ParseError::NO_ERROR) {}

bool TcpHeaders::from_bytes(const std::vector<uint8_t> &bytes) {
    if (bytes.size() < correlation_id.size() + 4) {
        parse_error = ParseError::ERR_MISSING_CORRELATION_ID;
        return false;
    }
    std::memcpy(correlation_id.begin(), bytes.data(), correlation_id.size());
    protocol_version = bytes[correlation_id.size()];
    if (protocol_version > PROTOCOL_VERSION) {
        parse_error = ParseError::ERR_UNSUPPORTED_VERSION;
        return false;
    }
    switch (bytes[correlation_id.size() + 1]) {
    case 0:
        type = RequestType::Append;
        break;
    case 1:
        type = RequestType::Fetch;
        break;
    default:
        parse_error = ParseError::ERR_UNKNOWN_TYPE;
        return false;
    }
    std::array<uint8_t, 2> flag_bytes;
    if (byteswap::is_big_endian()) {
        flag_bytes[0] = bytes[correlation_id.size() + 2];
        flag_bytes[1] = bytes[correlation_id.size() + 3];
    } else {
        flag_bytes[0] = bytes[correlation_id.size() + 3];
        flag_bytes[1] = bytes[correlation_id.size() + 2];
    }
    std::memcpy(&flags, &flag_bytes, sizeof(flags));
    if (flags != 0) {
        parse_error = ParseError::ERR_UNSUPPORTED_FLAGS;
        return false;
    }
    parse_error = ParseError::NO_ERROR;
    return true;
}

std::vector<uint8_t> TcpHeaders::to_bytes() const {
    size_t pos = 0;
    std::vector<uint8_t> bytes(TCP_REQUEST_HEADER_LEN);
    std::memcpy(bytes.data(), correlation_id.begin(), correlation_id.size());
    pos += correlation_id.size();
    std::memcpy(bytes.data() + pos, &protocol_version,
                sizeof(protocol_version));
    pos += sizeof(protocol_version);
    uint8_t type_byte = 0;
    switch (type) {
    case RequestType::Append:
        type_byte = 0;
        break;
    case RequestType::Fetch:
        type_byte = 1;
        break;
    }
    std::memcpy(bytes.data() + pos, &type_byte, sizeof(type_byte));
    pos += sizeof(type_byte);

    if (byteswap::is_big_endian()) {
        std::memcpy(bytes.data() + pos, &flags, sizeof(flags));
    } else {
        std::array<uint8_t, 2> flag_bytes;
        std::memcpy(flag_bytes.data(), &flags, sizeof(flags));
        bytes[pos] = flag_bytes[1];
        bytes[pos + 1] = flag_bytes[0];
    }
    return bytes;
}

std::variant<AppendRequest, FetchRequest> TcpRequest::to_specialized_type() {
    switch (headers.type) {
    case RequestType::Append:
        return AppendRequest{.correlation_id = headers.correlation_id,
                             .payload = payload};
    case RequestType::Fetch:
        FetchRequest request{.correlation_id = headers.correlation_id,
                             .offset = 0,
                             .max_bytes = 0};
        std::memcpy(&request.offset, payload.data(), sizeof(request.offset));
        std::memcpy(&request.max_bytes, payload.data() + sizeof(request.offset),
                    sizeof(request.max_bytes));
        if (byteswap::is_big_endian()) {
            request.offset = byteswap::byteswap64(request.offset);
            request.max_bytes = byteswap32(request.max_bytes);
        }
        return request;
    }
}

std::vector<uint8_t> TcpRequest::make_payload(uint64_t offset,
                                              uint32_t max_bytes) {
    std::vector<uint8_t> payload(sizeof(offset) + sizeof(max_bytes));
    if (byteswap::is_big_endian()) {
        offset = byteswap64(offset);
        max_bytes = byteswap32(max_bytes);
    }
    std::memcpy(payload.data(), &offset, sizeof(offset));
    std::memcpy(payload.data() + sizeof(offset), &max_bytes, sizeof(max_bytes));
    return payload;
}

std::vector<uint8_t> TcpResponse::to_bytes() const {
    uint32_t len = TCP_RESPONSE_HEADER_LEN;
    if (payload.has_value())
        len += payload.value().size();
    std::vector<uint8_t> bytes(len + sizeof(len));
    if (!byteswap::is_big_endian())
        len = byteswap::byteswap32(len);
    std::memcpy(bytes.data(), &len, sizeof(len));
    size_t pos = sizeof(len);
    std::memcpy(bytes.data() + pos, correlation_id.begin(),
                correlation_id.size());
    pos += correlation_id.size();
    std::memcpy(bytes.data() + pos, &response_code, sizeof(response_code));
    if (payload.has_value()) {
        pos += sizeof(response_code);
        std::memcpy(bytes.data() + pos, payload.value().data(),
                    payload.value().size());
    }
    return bytes;
}

TcpResponse TcpResponse::from_bytes(const std::vector<uint8_t> &bytes) {
    TcpResponse response;
    uint32_t len = 0;
    std::memcpy(&len, bytes.data(), sizeof(len));
    if (!byteswap::is_big_endian())
        len = byteswap32(len);
    if (len != bytes.size() - sizeof(len)) {
        std::string msg = "Encoded len is " + std::to_string(len) +
                          ", but size of bytes is " +
                          std::to_string(bytes.size() - sizeof(len)) + ".";
        throw std::logic_error(msg);
    }
    size_t pos = sizeof(len);
    std::memcpy(response.correlation_id.begin(), bytes.data() + pos,
                response.correlation_id.size());
    pos += response.correlation_id.size();
    std::memcpy(&response.response_code, bytes.data() + pos,
                sizeof(response.response_code));
    pos += sizeof(response.response_code);
    if (len > TCP_RESPONSE_HEADER_LEN) {
        response.payload.emplace(len - TCP_RESPONSE_HEADER_LEN);
        std::memcpy(response.payload->data(), bytes.data() + pos,
                    response.payload->size());
    }
    return response;
}

TcpResponse
TcpResponse::makeErrorResponse(const boost::uuids::uuid &correlation_id,
                                 ParseError error) {
    TcpResponse response;
    response.correlation_id = correlation_id;
    switch (error) {
    case ParseError::NO_ERROR:
        throw std::logic_error(
            "No error but TcpResponse::createErrorResponse called.");
        break;
    case ParseError::ERR_MISSING_CORRELATION_ID:
        response.correlation_id = {{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                    0x00, 0x00}};
        response.response_code = static_cast<uint8_t>(error);
        break;
    default:
        response.response_code = static_cast<uint8_t>(error);
        break;
    }
    response.payload.reset();
    return response;
}

TcpResponse TcpResponse::makeResponse(const boost::uuids::uuid &correlation_id,
                                      uint64_t offset,
                                      const std::error_code &ec) {
    TcpResponse response;
    response.correlation_id = correlation_id;
    if (ec) {
        if (ec.value() ==
            std::make_error_code(std::errc::not_connected).value())
            response.response_code = 0x80;
        else if (ec.value() ==
                 std::make_error_code(std::errc::io_error).value())
            response.response_code = 0x81;
        else
            response.response_code = 0xFF;
        response.payload.reset();
    } else {
        response.response_code = 0;
        response.payload.emplace(sizeof(offset));
        if (!is_big_endian())
            offset = byteswap64(offset);
        std::memcpy(response.payload->data(), &offset, sizeof(offset));
    }
    return response;
}

TcpResponse TcpResponse::makeResponse(const boost::uuids::uuid &correlation_id,
                                      const FetchResult &result,
                                      const std::error_code &ec) {
    TcpResponse response;
    response.correlation_id = correlation_id;
    if (ec) {
        if (ec.value() ==
            std::make_error_code(std::errc::not_connected).value())
            response.response_code = 0x80;
        else if (ec.value() ==
                 std::make_error_code(std::errc::io_error).value())
            response.response_code = 0x81;
        else
            response.response_code = 0xFF;
        response.payload.reset();
    } else {
        response.response_code = 0;
        response.payload.emplace(result.result_buf);
    }
    return response;
}

} // namespace broker
} // namespace kafka_lite
