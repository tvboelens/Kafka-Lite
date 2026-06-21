#include "../include/BrokerCore.h"
#include "../include/RecordManager.h"
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <queue>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace kafka_lite {
namespace broker {

using namespace std::chrono_literals;

struct AppendedRecord {
    uint64_t offset;
    Record record;
};

class TestMtAppender : public std::enable_shared_from_this<TestMtAppender> {
  public:
    TestMtAppender(std::unique_ptr<BrokerCore> &core)
        : stopped_(false), core_(core) {}
    ~TestMtAppender() { stop(); }
    void stop() {
        if (!stopped_) {
            for (auto &thread : append_threads_) {
                if (thread.joinable())
                    thread.join();
            }
        }
        stopped_ = true;
    }
    void start() {
        for (auto &queue : append_queues_) {
            append_threads_.push_back(std::thread(
                [append_queue = std::move(queue), self = shared_from_this()]() {
                    self->append_loop(append_queue);
                }));
        }
        stopped_ = false;
    }
    std::vector<AppendedRecord> get_appended_records() {
        return appended_records_;
    }
    void set_append_queue(std::vector<std::queue<Record>> &append_queues) {
        append_queues_ = std::move(append_queues);
    }

  private:
    void append_loop(std::queue<Record> append_queue);
    void handle_appended_record(Record record, uint64_t offset,
                                std::error_code ec) {
        if (ec) {
            return;
        }
        appended_records_.push_back({offset, record});
    }

    std::vector<std::thread> append_threads_;
    std::vector<std::queue<Record>> append_queues_;
    std::vector<AppendedRecord> appended_records_;
    std::unique_ptr<BrokerCore> &core_;
    bool stopped_;
};

void TestMtAppender::append_loop(std::queue<Record> append_queue) {
    Record record;
    while (true) {
        if (append_queue.empty()) {
            break;
        }

        record = append_queue.front();
        append_queue.pop();
        core_->submit_append(
            {record.to_bytes()}, [self = shared_from_this(),
                                  record](uint64_t offset, std::error_code ec) {
                self->handle_appended_record(record, offset, ec);
            });
    }
}

class TestMtFetcher : public std::enable_shared_from_this<TestMtFetcher> {
  public:
    TestMtFetcher(std::unique_ptr<BrokerCore> &core, unsigned int no_of_threads)
        : stopped_(false), no_of_threads_(no_of_threads), core_(core) {}
    void stop() {
        if (!stopped_) {
            stopped_ = true;
            for (auto &thread : fetch_threads_) {
                if (thread.joinable())
                    thread.join();
            }
        }
    }
    void start() {
        stopped_ = false;
        for (unsigned int i = 0; i < no_of_threads_; ++i) {
            fetch_threads_.push_back(std::thread(
                [self = shared_from_this()]() { self->fetch_loop(); }));
        }
    }
    std::vector<std::vector<Record>> get_fetched_records() {
        return fetched_records_;
    }

  private:
    void fetch_loop();

    std::mutex mutex_;
    unsigned int no_of_threads_;
    std::vector<std::thread> fetch_threads_;
    std::vector<std::vector<Record>> fetched_records_;
    std::unique_ptr<BrokerCore> &core_;
    bool stopped_;
};

void TestMtFetcher::fetch_loop() {
    uint64_t last_offset = 0;
    std::vector<Record> fetched_records;
    while (!stopped_) {
        std::vector<uint8_t> result_buf;
        core_->submit_fetch({.offset = last_offset, .max_bytes = 8192},
                            [&](const FetchResult &result, std::error_code ec) {
                                result_buf = result.result_buf;
                            });
        auto fetch_result = RecordManager::extract_records(result_buf);
        last_offset += fetch_result.size() + 1;
        fetched_records.reserve(fetched_records.size() + fetch_result.size());
        fetched_records.insert(fetched_records.end(), fetch_result.begin(),
                               fetch_result.end());
    }
    std::lock_guard lock(mutex_);
    fetched_records_.push_back(fetched_records);
}

class BrokerCoreTests : public ::testing::Test {
  private:
    std::filesystem::path dir_;

  public:
    std::vector<std::queue<Record>>
    generate_records(size_t record_len, unsigned int no_of_records,
                     unsigned int no_of_threads) {
        std::vector<std::queue<Record>> records(no_of_threads);
        for (unsigned int i = 0; i < no_of_records; ++i) {
            std::vector<uint8_t> payload;
            for (unsigned int j = i; j < i + record_len; ++j) {
                payload.push_back(j % record_len);
            }
            records[i % no_of_threads].push(
                RecordManager::create_record(payload));
        }
        return records;
    }

  protected:
    std::unique_ptr<BrokerCore> core_;
    void SetUp() override {
        dir_ = std::filesystem::current_path() / "Core";
        core_ = std::make_unique<BrokerCore>(dir_, 2048);
        core_->start();
    }
    void TearDown() override {
        core_->stop();
        std::filesystem::remove_all(dir_);
    }
};

TEST_F(BrokerCoreTests, mt_append_read_result_after) {
    auto append_queue = generate_records(100, 1000, 4);
    auto appender = std::make_shared<TestMtAppender>(core_);
    appender->set_append_queue(append_queue);
    appender->start();
    while (appender.use_count() > 1) {
        std::this_thread::sleep_for(10ms);
    }
    auto appended_records = appender->get_appended_records();

    ASSERT_EQ(appended_records.size(), 1000);
    for (auto it = appended_records.begin() + 1; it != appended_records.end();
         ++it) {
        EXPECT_TRUE((it - 1)->offset < it->offset);
    }
    std::vector<uint8_t> result_buf;
    core_->submit_fetch({.offset = 0, .max_bytes = 1000000},
                        [&](const FetchResult &result, std::error_code ec) {
                            result_buf = result.result_buf;
                        });
    auto fetched_records = RecordManager::extract_records(result_buf);

    ASSERT_EQ(appended_records.size(), fetched_records.size());
    for (auto it = appended_records.begin(); it != appended_records.end();
         ++it) {
        EXPECT_EQ(it->record.checksum,
                  fetched_records[it - appended_records.begin()].checksum);
        EXPECT_EQ(it->record.payload,
                  fetched_records[it - appended_records.begin()].payload);
    }
}

TEST_F(BrokerCoreTests, mt_append_mt_fetch_during) {
    auto fetcher = std::make_shared<TestMtFetcher>(core_, 8);
    auto append_queue = generate_records(100, 1000, 4);
    auto appender = std::make_shared<TestMtAppender>(core_);
    appender->set_append_queue(append_queue);
    fetcher->start();
    appender->start();
    while (appender.use_count() > 1) {
        std::this_thread::sleep_for(10ms);
    }
    fetcher->stop();
    auto appended_records = appender->get_appended_records();

    ASSERT_EQ(appended_records.size(), 1000);
    for (auto it = appended_records.begin() + 1; it != appended_records.end();
         ++it) {
        EXPECT_TRUE((it - 1)->offset < it->offset);
    }
    auto fetched_records = fetcher->get_fetched_records();
    for (auto &records : fetched_records) {
        ASSERT_TRUE(records.size() < appended_records.size());
        for (auto it = records.begin(); it != records.end(); ++it) {
            EXPECT_EQ(it->payload,
                      appended_records[it - records.begin()].record.payload);
        }
    }
}

/*
1. Multithreaded appends (with and without rollover)
    1. n threads appending
    2. record results (i.e. offsets) in n vectors -> todo: write offset into the
record itself
    3. ensure that offsets have increased monotonically
    4. Compare contents?
2. Multithreaded reads (with and without rollover):
    1. same as appending, but now start reading during writes
    2. Assert monotonous offsets, maybe compare contents for the returned
records
3. Any way to test stopping?
    1. Right now no stop function is implemented, so need to do that first
    2. Need to make sure that a stop leads to complete reads or writes, so
especially for reads I need to be careful. For this I first need to know that
crash recovery indeed takes place (is called), since I need to restart to read
again.
4. Maybe also a small test for starting that crash recovery has taken place.
   This does not have to be exhaustive, i.e. just do some single threaded
writes, ensure these are correct. Then restart core and ensure contents are
still there, i.e. no torn writes or anything.
*/

} // namespace broker
} // namespace kafka_lite
