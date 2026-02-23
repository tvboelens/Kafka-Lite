#ifndef BROKERCORE_H
#define BROKERCORE_H

#include "AppendQueue.h"
#include "Log.h"
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <thread>

namespace kafka_lite {
namespace broker {

enum class BrokerCoreStatus { Starting, Recovering, Active, Stopping, Stopped };

class BrokerCore {
  public:
    BrokerCore(const std::filesystem::path &dir, uint64_t segment_size);
    ~BrokerCore();
    
    void submit_append(const AppendData &data, AppendCallback callback);
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
