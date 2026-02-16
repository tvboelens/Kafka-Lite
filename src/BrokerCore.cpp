#include "../include/BrokerCore.h"
#include <cstdint>
#include <exception>
#include <future>
#include <system_error>

namespace kafka_lite {
namespace broker {

void BrokerCore::submit_append(const AppendData &data, AppendCallback callback) {
    // TODO: if stopped, don't enqueue anymore, but return error/throw exception
    AppendJob job;
    job.payload = data.data;
    job.callback = callback;
    append_queue_.push(job);
}

void BrokerCore::writerLoop() {
    while (!stop_.load()) {
        AppendJob job;
        append_queue_.wait_and_pop(job);
        AppendData append_data{job.payload};
        std::error_code ec;
        uint64_t offset;
        try {
            offset = append_log_.append(append_data);
        } catch (const std::exception &e) {
            ec = make_error_code(std::errc::io_error);
        }

        job.callback(offset, ec);
    }
}
} // namespace broker
} // namespace kafka_lite
