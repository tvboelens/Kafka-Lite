#include "../include/BrokerCore.h"
#include <cstdint>
#include <exception>
#include <system_error>

namespace kafka_lite {
namespace broker {

BrokerCore::BrokerCore(const std::filesystem::path &dir, uint64_t segment_size)
    : status_(BrokerCoreStatus::Starting), stop_(false),
      append_log_(dir, segment_size),
      writer_thread(&BrokerCore::writerLoop, this) {}

BrokerCore::~BrokerCore() {
    stop_.store(true);
    if (writer_thread.joinable())
        writer_thread.join();
}

void BrokerCore::start() {
    append_log_.start();
    status_ = BrokerCoreStatus::Active;
}

void BrokerCore::submit_append(const AppendData &data,
                               AppendCallback callback) {
    if (status_ == BrokerCoreStatus::Stopping ||
        status_ == BrokerCoreStatus::Stopped) {
        std::error_code ec = std::make_error_code(std::errc::not_connected);
        callback(0, ec); // TODO: maybe here boost::asio::post already needs to be called?
        return;
    } else if (status_ == BrokerCoreStatus::Starting ||
               status_ == BrokerCoreStatus::Recovering) {
        while (status_ != BrokerCoreStatus::Active);// Block appends to avoid appends during recovery
	}
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
