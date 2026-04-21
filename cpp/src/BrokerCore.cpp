#include "../include/BrokerCore.h"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <system_error>
#include <thread>

using namespace std::chrono_literals;

namespace kafka_lite {
namespace broker {

BrokerCore::BrokerCore(const std::filesystem::path &dir, uint64_t segment_size)
    : status_(BrokerCoreStatus::Starting), stop_(false),
      append_log_(dir, segment_size),
      writer_thread(&BrokerCore::writerLoop, this) {}

BrokerCore::~BrokerCore() {
    stop();
}

void BrokerCore::start() {
    append_log_.start();
    status_ = BrokerCoreStatus::Active;
}

void BrokerCore::stop() {
    status_ = BrokerCoreStatus::Stopping;
    stop_.store(true);
    if (writer_thread.joinable())
        writer_thread.join();
    while (fetch_calls_counter_.load(std::memory_order_acquire) > 0)
        std::this_thread::sleep_for(50ms);
    status_ = BrokerCoreStatus::Stopped;
}

void BrokerCore::submit_append(const AppendData &data,
                               AppendCallback callback) {
    if (status_ == BrokerCoreStatus::Stopping ||
        status_ == BrokerCoreStatus::Stopped) {
        std::error_code ec = std::make_error_code(std::errc::not_connected);
        callback(0, ec);
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

void BrokerCore::submit_fetch(const FetchRequest &request,
                              FetchCallback callback) {
    auto counter = fetch_calls_counter_.fetch_add(1, std::memory_order_acq_rel);
    if (status_ == BrokerCoreStatus::Stopping ||
        status_ == BrokerCoreStatus::Stopped) {
        std::error_code ec = std::make_error_code(std::errc::not_connected);
        callback({}, ec);
        counter = fetch_calls_counter_.fetch_sub(1, std::memory_order_release);
        return;
    } else if (status_ == BrokerCoreStatus::Starting ||
               status_ == BrokerCoreStatus::Recovering) {
        while (status_ != BrokerCoreStatus::Active)
            std::this_thread::sleep_for(300ms); // Block reads to avoid appends during recovery
    }
    std::error_code ec;
    FetchResult result;
    try {
        result = append_log_.fetch(request);
    } catch (const std::exception &e) {
        ec = make_error_code(std::errc::io_error);
    }
    callback(result, ec);
    counter = fetch_calls_counter_.fetch_sub(1, std::memory_order_release);
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
