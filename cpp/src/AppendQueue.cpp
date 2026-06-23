#include "../include/AppendQueue.h"
#include <chrono>
#include <ctime>
#include <mutex>
#include <vector>

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

std::vector<AppendJob> AppendQueue::wait_and_pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    bool job_queue_nonempty =
        cv_.wait_for(lock, std::chrono::milliseconds(5),
                     [this] { return jobs_.size() > 10; });
    if (jobs_.empty())
        return {};
    std::vector<AppendJob> result;
    result.reserve(jobs_.size());
    while (!jobs_.empty()) {
        result.push_back(std::move(jobs_.front()));
        jobs_.pop();
    }

    return result;
}
} // namespace broker
} // namespace kafka_lite
