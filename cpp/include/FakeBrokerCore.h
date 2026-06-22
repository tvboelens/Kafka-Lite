#ifndef FAKE_BROKERCORE_HH
#define FAKE_BROKERCORE_HH

#include "BrokerCoreIfc.h"
#include <cstdint>
#include <shared_mutex>
#include <vector>

namespace kafka_lite {
namespace broker {

class FakeBrokerCore : public BrokerCoreIfc {
  public:
    FakeBrokerCore();
    void submit_append(const AppendData &data,
                       AppendCallback callback) override;
    void submit_fetch(const FetchData &data, FetchCallback callback) override;
    void start() override;
    void stop() override;
    uint64_t get_published_offset() override;

  private:
    std::vector<std::vector<uint8_t>> records_;
    std::shared_mutex records_mutex_;
    bool stop_;
};

} // namespace broker
} // namespace kafka_lite

#endif
