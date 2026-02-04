#include "../include/BrokerCore.h"
#include <cstdint>
#include <future>

namespace kafka_lite {
namespace broker {

AppendResult BrokerCore::handleAppendRequest(const AppendRequest &request) {
    // TODO: if stopped, don't enqueue anymore, but return error/throw exception
    AppendJob job;
    job.payload = request.payload;
    std::future<uint64_t> offset_future = job.result.get_future();
    append_queue_.push(job);
    return AppendResult{offset_future.get()};
}

void BrokerCore::writerLoop() {
    while (!stop_.load()) {
        AppendJob job;
        append_queue_.wait_and_pop(job);
        AppendData append_data;
        append_data.data = job.payload;
        uint64_t offset = append_log_.append(append_data);
        job.result.set_value(offset);
    }
}
} // namespace broker
} // namespace kafka_lite
