#include "../include/ByteSwap.h"
#include "../include/RecordProducer.h"
#include <boost/crc.hpp>
#include <cstdint>
#include <cstring>
#include <vector>



namespace kafka_lite {
namespace broker {

using namespace kafka_lite::byteswap;

using crc32c_type =
    boost::crc_optimal<32, 0x1EDC6F41, 0xFFFFFFFF, 0xFFFFFFFF, true, true>;


std::vector<uint8_t> RecordProducer::create_record(const std::vector<uint8_t> &payload) {
    crc32c_type crc32c;
    crc32c.process_bytes(payload.data(), payload.size());
    uint32_t checksum = crc32c.checksum();
    std::vector<uint8_t> record(payload.size() + sizeof(checksum));
    if (byteswap::is_big_endian())
        checksum = byteswap::byteswap32(checksum);
    std::memcpy(record.data(), &checksum, sizeof(checksum));
    std::memcpy(record.data() + sizeof(checksum), payload.data(), payload.size());
    return record;
}

}
}
