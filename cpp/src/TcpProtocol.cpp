#include "../include/ByteSwap.h"
#include "../include/TcpProtocol.h"
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace kafka_lite {
namespace broker {

using namespace kafka_lite::byteswap;

bool TcpHeaders::from_bytes(const std::vector<uint8_t> &bytes) {
    if (bytes.size() < correlation_id.size() + 4)
        return false;
    std::memcpy(correlation_id.begin(), bytes.data(), correlation_id.size());
    protocol_version = bytes[correlation_id.size()];
    switch (bytes[correlation_id.size() + 1]) {
    case 0:
        type = RequestType::Append;
        break;
    case 1:
        type = RequestType::Fetch;
        break;
    default:
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
    return true;
}

std::variant<AppendRequest, FetchRequest> TcpRequest::to_specialized_type() {
    switch (headers.type) {
    case RequestType::Append:
        return AppendRequest{payload};
    case RequestType::Fetch:
        uint64_t offset;
        std::memcpy(&offset, payload.data(), sizeof(offset));
        if (!byteswap::is_big_endian())
            offset = byteswap::byteswap64(offset);
        uint32_t max_bytes;
        std::memcpy(&offset, payload.data()+sizeof(offset), sizeof(max_bytes));
        return FetchRequest{offset, max_bytes};
    }
}

std::vector<uint8_t> TcpHeaders::to_bytes() {
    size_t pos = 0;
    std::vector<uint8_t> bytes(TCP_REQUEST_HEADER_LEN);
    std::memcpy(bytes.data(), correlation_id.begin(), correlation_id.size());
    pos += correlation_id.size();
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
    pos += 1;

    
    if (byteswap::is_big_endian()) {
        std::memcpy(bytes.data() + pos, &flags, sizeof(flags));
    } else {
        std::array<uint8_t, 2> flag_bytes;
        std::memcpy(flag_bytes.data(), &flags, sizeof(flags));
        bytes[pos - 1] = flag_bytes[1];
        bytes[pos] = flag_bytes[0];
    }
    return bytes;
}

std::vector<uint8_t> TcpResponse::to_bytes() {
    uint32_t len = TCP_RESPONSE_HEADER_LEN, pos = 0;
    if (payload.has_value())
        len += payload.value().size();
    if (!byteswap::is_big_endian())
        len = byteswap::byteswap32(len);
    std::vector<uint8_t> bytes(len + sizeof(len));
    std::memcpy(bytes.data(), &len, sizeof(len));
    std::memcpy(bytes.data() + sizeof(len), correlation_id.begin(),
                correlation_id.size());
    std::memcpy(bytes.data() + sizeof(len) + correlation_id.size(), &response_code,
                sizeof(response_code));
    return bytes;
}
}
}
