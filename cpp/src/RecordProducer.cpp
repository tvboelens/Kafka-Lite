#include "../include/RecordProducer.h"
#include "../include/ByteSwap.h"
#include <boost/crc.hpp>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace kafka_lite {
namespace broker {

using namespace kafka_lite::byteswap;

using crc32c_type =
    boost::crc_optimal<32, 0x1EDC6F41, 0xFFFFFFFF, 0xFFFFFFFF, true, true>;

std::vector<uint8_t> Record::to_bytes() {
    std::vector<uint8_t> bytes(payload.size() + sizeof(checksum));
    if (byteswap::is_big_endian())
        checksum = byteswap::byteswap32(checksum);
    std::memcpy(bytes.data(), &checksum, sizeof(checksum));
    std::memcpy(bytes.data() + sizeof(checksum), payload.data(),
                payload.size());
    return bytes;
}

std::vector<uint8_t> Record::to_bytes_with_len() {
    uint32_t len = payload.size() + sizeof(checksum);
    size_t pos = 0;
    std::vector<uint8_t> bytes(payload.size() + sizeof(checksum) + sizeof(len));
    if (byteswap::is_big_endian()) {
        checksum = byteswap::byteswap32(checksum);
        len = byteswap::byteswap32(len);
    }
    std::memcpy(bytes.data() + pos, &len, sizeof(len));
    pos += sizeof(len);
    std::memcpy(bytes.data() + pos, &checksum, sizeof(checksum));
    pos += sizeof(checksum);
    std::memcpy(bytes.data() + pos, payload.data(), payload.size());
    return bytes;
}

Record RecordProducer::create_record(const std::vector<uint8_t> &payload) {
    crc32c_type crc32c;
    crc32c.process_bytes(payload.data(), payload.size());
    uint32_t checksum = crc32c.checksum();
    return {.checksum = checksum, .payload = payload};
}

std::vector<Record>
RecordProducer::extract_records(const std::vector<uint8_t> bytes) {
    size_t pos = 0;
    std::vector<Record> result;
    while (pos < bytes.size()) {
        uint32_t len = 0, checksum = 0;
        std::memcpy(&len, bytes.data() + pos, sizeof(len));
        pos += sizeof(len);
        std::memcpy(&checksum, bytes.data() + pos, sizeof(checksum));
        pos += sizeof(checksum);
        if (byteswap::is_big_endian()) {
            len = byteswap::byteswap32(len);
            checksum = byteswap::byteswap32(checksum);
        }
        std::vector<uint8_t> payload(len - sizeof(checksum));
        std::memcpy(payload.data(), bytes.data() + pos, payload.size());
        pos += payload.size();
        result.push_back({.checksum = checksum, .payload = payload});
    }
    return result;
}

} // namespace broker
} // namespace kafka_lite
