#ifndef BROKERCORE_H
#define BROKERCORE_H

#include "AppendQueue.h"
#include "Log.h"
#include <atomic>
#include <cstdint>
#include <vector>

struct AppendRequest {
    std::vector<uint8_t> payload;
};
struct AppendResult {
    uint64_t offset;
};

class BrokerCore {
  public:
    AppendResult handleAppendRequest(const AppendRequest &request);
  private:
    AppendQueue append_queue_;
    Log append_log_;
    void writerLoop();
    std::atomic_bool stop_;
};

#endif
