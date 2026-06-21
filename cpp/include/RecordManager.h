#ifndef RecordManager_HH
#define RecordManager_HH

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

class RecordManager {
  public:
    static Record create_record(const std::vector<uint8_t> &payload);
    static std::vector<Record>
    extract_records(const std::vector<uint8_t> bytes);
    static bool check_integrity(const std::vector<uint8_t> &bytes);
    static bool check_integrity_with_len(const std::vector<uint8_t> &bytes);
    static bool check_integrity(const Record &record);
};

} // namespace broker
} // namespace kafka_lite

#endif
