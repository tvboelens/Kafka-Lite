#include "../include/AppendQueue.h"
#include <chrono>
#include <mutex>

namespace kafka_lite {
namespace broker {

AppendJob::AppendJob(AppendJob &&job) noexcept
    : payload(job.payload), callback(job.callback) {}

AppendJob &AppendJob::operator=(AppendJob &&job) noexcept {
    if (&job == this)
        return *this;
    payload = job.payload;
    callback = job.callback;
    return *this;
}

void AppendQueue::push(AppendJob &job) {
    std::lock_guard lock(mutex_);
    jobs_.push(std::move(job));
    cv_.notify_one();
}

bool AppendQueue::wait_and_pop(AppendJob &job) {
    std::unique_lock<std::mutex> lock(mutex_);
    bool job_queue_nonempty = cv_.wait_for(lock, std::chrono::milliseconds(10),
                                           [this] { return !jobs_.empty(); });
    if (!job_queue_nonempty)
        return false;
    job = std::move(jobs_.front());
    jobs_.pop();
    return true;
}
} // namespace broker
} // namespace kafka_lite
