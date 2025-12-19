#ifndef APPENDQUEUE_H
#define APPENDQUEUE_H

#include <future>
#include <vector>
#include <queue>

struct AppendJob
{
	AppendJob(AppendJob &job_);
	AppendJob(const AppendJob &job_) = delete;
	AppendJob& operator=(const AppendJob &job_) = delete;
	AppendJob(AppendJob &&job_) noexcept;
	AppendJob &operator=(AppendJob &&job_) noexcept;

	~AppendJob() = default;

	std::vector<uint8_t> payload;
	std::promise<uint64_t> result;
};

class AppendQueue
{
	public:
		AppendQueue();
		~AppendQueue();

		void push(AppendJob &job);
		AppendJob pop();
	private:
		std::queue<AppendJob> jobs_;
		std::mutex mutex_;
};

#endif
