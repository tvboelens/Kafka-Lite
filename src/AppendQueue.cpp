#include "../include/AppendQueue.h"

AppendJob::AppendJob(AppendJob &job) : payload(job.payload), result(std::move(job.result)) {}

AppendJob::AppendJob(AppendJob &&job) noexcept : payload(job.payload), result(std::move(job.result)) {}

AppendJob& AppendJob::operator=(AppendJob &&job) noexcept {
	if (&job == this)
		return *this;
	payload = job.payload;
	result = std::move(job.result);
	return *this;
}

void AppendQueue::push(AppendJob &job) {
	std::lock_guard lock(mutex_);
	jobs_.push(std::move(job));
}

AppendJob AppendQueue::pop() {
	std::lock_guard lock(mutex_);
	AppendJob job = jobs_.front();
	jobs_.pop();
	return job;
}
