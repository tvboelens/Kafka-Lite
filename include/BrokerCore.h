#ifndef BROKERCORE_H
#define BROKERCORE_H

#include "AppendQueue.h"
#include "Log.h"
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <thread>
#include <vector>

namespace kafka_lite {
namespace broker {

class BrokerCore {
  public:
    BrokerCore(const std::filesystem::path &dir, uint64_t segment_size): stop_(false), append_log_(dir, segment_size), writer_thread(&BrokerCore::writerLoop, this) {}
    void submit_append(const AppendData &data, AppendCallback callback);
	~BrokerCore(){stop_.store(true); if (writer_thread.joinable()) writer_thread.join();}
  private:
    AppendQueue append_queue_;
    Log append_log_;
    void writerLoop();
    std::thread writer_thread;
    std::atomic_bool stop_;
};
} // namespace broker
} // namespace kafka_lite
#endif
