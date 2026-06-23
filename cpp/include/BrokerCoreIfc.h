#ifndef BROKERCORE_IFC_H
#define BROKERCORE_IFC_H

#include "AppendQueue.h"
#include "Log.h"
#include <cstdint>
#include <functional>

namespace kafka_lite {
namespace broker {

using FetchCallback = std::function<void(const FetchResult &, std::error_code)>;

class BrokerCoreIfc {
  public:
    virtual ~BrokerCoreIfc() {}
    virtual void submit_append(const AppendData &data,
                               AppendCallback callback) = 0;
    virtual void submit_fetch(const FetchData &data,
                              FetchCallback callback) = 0;
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual uint64_t get_published_offset() = 0;
};

} // namespace broker
} // namespace kafka_lite

#endif
