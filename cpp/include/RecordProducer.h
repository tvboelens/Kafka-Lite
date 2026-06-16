#ifndef RECORDPRODUCER_HH
#define RECORDPRODUCER_HH

#include <cstdint>
#include <vector>
namespace kafka_lite {
namespace broker {

class RecordProducer {
  public:
    static std::vector<uint8_t> create_record(const std::vector<uint8_t> &payload);
};


}
}

#endif
