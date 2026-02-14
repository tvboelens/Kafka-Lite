#ifndef APPENDQUEUE_H
#define APPENDQUEUE_H

#include <condition_variable>
#include <future>
#include <queue>
#include <vector>

namespace kafka_lite {
namespace broker {
struct AppendJob {
    AppendJob() = default;
    AppendJob(AppendJob &job_) = delete;
    AppendJob(const AppendJob &job_) = delete;
    AppendJob &operator=(const AppendJob &job_) = delete;
    AppendJob(AppendJob &&job_) noexcept;
    AppendJob &operator=(AppendJob &&job_) noexcept;

    ~AppendJob() = default;

    std::vector<uint8_t> payload;
    std::promise<uint64_t> result;
};

class AppendQueue {
  public:
    void push(AppendJob &job);
    void wait_and_pop(AppendJob &job);

  private:
    std::queue<AppendJob> jobs_;
    std::mutex mutex_;
    std::condition_variable cv_;
};
} // namespace broker
} // namespace kafka_lite

#endif
