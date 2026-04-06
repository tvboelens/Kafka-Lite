#ifndef BROKERCORE_H
#define BROKERCORE_H

#include "AppendQueue.h"
#include "Log.h"
#include "Segment.h"
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <thread>

namespace kafka_lite {
namespace broker {

using FetchCallback = std::function<void(const FetchResult &, std::error_code)>;

enum class BrokerCoreStatus { Starting, Recovering, Active, Stopping, Stopped };

class BrokerCore {
  public:
    BrokerCore(const std::filesystem::path &dir, uint64_t segment_size);
    ~BrokerCore();

    void submit_append(const AppendData &data, AppendCallback callback);
    void submit_fetch(const FetchRequest &request, FetchCallback callback);
    void start();
  private:
    void writerLoop();
    
    AppendQueue append_queue_;
    Log append_log_;
    BrokerCoreStatus status_;
    std::thread writer_thread;
    std::atomic_bool stop_;
};
} // namespace broker
} // namespace kafka_lite
#endif
