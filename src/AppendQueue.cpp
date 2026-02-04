#include "../include/AppendQueue.h"
#include <mutex>

namespace kafka_lite {
namespace broker {

AppendJob::AppendJob(AppendJob &job)
    : payload(job.payload), result(std::move(job.result)) {}

AppendJob::AppendJob(AppendJob &&job) noexcept
    : payload(job.payload), result(std::move(job.result)) {}

AppendJob &AppendJob::operator=(AppendJob &&job) noexcept {
    if (&job == this)
        return *this;
    payload = job.payload;
    result = std::move(job.result);
    return *this;
}

void AppendQueue::push(AppendJob &job) {
    std::lock_guard lock(mutex_);
    jobs_.push(std::move(job));
    cv_.notify_one();
}

void AppendQueue::wait_and_pop(AppendJob &job) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return !jobs_.empty(); });
    job = std::move(jobs_.front());
    jobs_.pop();
}
} // namespace broker
} // namespace kafka_lite
