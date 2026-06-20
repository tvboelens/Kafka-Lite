#ifndef RECORDPRODUCER_HH
#define RECORDPRODUCER_HH

#include <cstdint>
#include <vector>
namespace kafka_lite {
namespace broker {

struct Record {
    uint32_t checksum;
    std::vector<uint8_t> payload;

    std::vector<uint8_t> to_bytes();
    std::vector<uint8_t> to_bytes_with_len();
};

class RecordProducer {
  public:
    static Record create_record(const std::vector<uint8_t> &payload);
    static std::vector<Record>
    extract_records(const std::vector<uint8_t> bytes);
};

} // namespace broker
} // namespace kafka_lite

#endif
