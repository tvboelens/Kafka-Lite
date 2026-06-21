#include "../include/FakeBrokerCore.h"
#include "../include/RecordManager.h"
#include <cstddef>
#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <system_error>

namespace kafka_lite {
namespace broker {

FakeBrokerCore::FakeBrokerCore() : stop_(true) {}

void FakeBrokerCore::start() { stop_ = false; }
void FakeBrokerCore::stop() {
    std::unique_lock<std::shared_mutex> lock(records_mutex_);
    stop_ = true;
}

void FakeBrokerCore::submit_append(const AppendData &data,
                                   AppendCallback callback) {
    std::unique_lock<std::shared_mutex> lock(records_mutex_);
    if (stop_) {
        callback(0, std::make_error_code(std::errc::not_connected));
        return;
    }
    if (!RecordManager::check_integrity_with_len(data.data)) {
        callback(0, std::make_error_code(std::errc::bad_message));
        return;
    }
    records_.push_back(data.data);
    std::error_code ec;
    callback(records_.size() - 1, ec);
}

void FakeBrokerCore::submit_fetch(const FetchData &data,
                                  FetchCallback callback) {
    std::unique_lock<std::shared_mutex> lock(records_mutex_);
    if (stop_) {
        callback({}, std::make_error_code(std::errc::not_connected));
        return;
    }
    FetchResult result;
    std::error_code ec;
    size_t i = data.offset;
    while (i < records_.size()) {
        if (result.result_buf.size() + records_[i].size() > data.max_bytes) {
            break;
        } else {
            auto old_size = result.result_buf.size();
            result.result_buf.resize(old_size + records_[i].size());
            std::memcpy(result.result_buf.data() + old_size, records_[i].data(),
                        records_[i].size());
            ++i;
        }
    }
    callback(result, ec);
}

} // namespace broker
} // namespace kafka_lite
