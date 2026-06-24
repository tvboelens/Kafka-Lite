#include "../include/ByteSwap.h"
#include "../include/Log.h"
#include "../include/RecordManager.h"
#include "../include/Segment.h"
#include <algorithm>
#include <boost/crc.hpp>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <gtest/gtest.h>
#include <limits>
#include <optional>
#include <vector>

namespace kafka_lite {
namespace broker {

using namespace byteswap;

std::vector<Record> generate_records(size_t record_len,
                                     unsigned int no_of_records) {
    std::vector<Record> records;
    for (unsigned int i = 0; i < no_of_records; ++i) {
        std::vector<uint8_t> payload;
        for (unsigned int j = i; j < i + record_len; ++j) {
            payload.push_back(j % 256);
        }
        records.push_back(RecordManager::create_record(payload));
    }
    return records;
}

class StorageEngineTests : public ::testing::Test {
  private:
    std::filesystem::path dir_;

  public:
    std::filesystem::path getDir() { return dir_; }

  protected:
    void SetUp() override {
        dir_ = std::filesystem::current_path() / "StorageEngine";
        std::filesystem::remove_all(dir_);
    }
    void TearDown() override;
};

void StorageEngineTests::TearDown() { std::filesystem::remove_all(dir_); }

TEST_F(StorageEngineTests, SegmentRW) {
    std::filesystem::path dir = getDir() / "SegmentRW";
    Segment segment(dir, 0, 32, SegmentState::Active);
    SegmentReadResult result = segment.read(0, 32);
    ASSERT_TRUE(result.result_buf.empty());
    std::vector<uint8_t> data, result_buf;
    data.reserve(4);
    for (int i = 1; i < 5; ++i) {
        data.push_back(i);
    }
    uint64_t offset =
        segment.append(data.data(), data.size() * sizeof(uint8_t));
    result = segment.read(offset, 32);
    result_buf.reserve(data.size());
    for (auto it = result.result_buf.begin() + SEGMENT_HEADER_SIZE;
         it != result.result_buf.end(); ++it) {
        result_buf.push_back(*it);
    }
    EXPECT_EQ(result_buf, data);
    EXPECT_EQ(segment.getBaseOffset(), segment.getPublishedOffset());
    EXPECT_EQ(result.last_read_offset, segment.getBaseOffset());
    EXPECT_EQ(result.last_read_offset, segment.getPublishedOffset());
};

TEST_F(StorageEngineTests, SegmentRWMultiple) {
    std::filesystem::path dir = getDir() / "SegmentRWMultiple";
    std::vector<uint8_t> data;
    std::vector<std::vector<uint8_t>> datav;
    datav.reserve(8);
    data.resize(4);
    std::vector<uint64_t> offsets;
    SegmentReadResult result;
    {
        Segment segment(dir, 0, 32, SegmentState::Active);

        offsets.resize(8);
        data.reserve(4);
        for (int i = 0; i < 8; ++i) {
            for (int j = 0; j < 4; ++j) {
                data[j] = i * 4 + j;
            }
            datav.push_back(data);
            offsets[i] =
                segment.append(data.data(), data.size() * sizeof(uint8_t));
        }
        for (int i = 0; i < 8; ++i) {
            EXPECT_EQ(offsets[i], i);
        }
        EXPECT_EQ(segment.getPublishedOffset(), 7);
        EXPECT_TRUE(segment.isFull());
        result = segment.read(7, 256);
        EXPECT_EQ(result.last_read_offset, 7);
        ASSERT_EQ(result.result_buf.size(), 4 + SEGMENT_HEADER_SIZE);
        EXPECT_EQ(0, std::memcmp(datav[7].data(),
                                 result.result_buf.data() + SEGMENT_HEADER_SIZE,
                                 4 * sizeof(uint8_t)));

        result = segment.read(4, 256);
        EXPECT_EQ(result.last_read_offset, 7);
        ASSERT_EQ(result.result_buf.size(), 4 * (4 + SEGMENT_HEADER_SIZE));
        for (int i = 4; i < 8; ++i) {
            EXPECT_EQ(
                0, std::memcmp(datav[i].data(),
                               result.result_buf.data() +
                                   (4 * sizeof(uint8_t) + SEGMENT_HEADER_SIZE) *
                                       (i - 4) +
                                   SEGMENT_HEADER_SIZE,
                               4 * sizeof(uint8_t)));
        }

        result = segment.read(8, 256);
        EXPECT_EQ(result.last_read_offset, 0);
        EXPECT_TRUE(result.result_buf.empty());
    }

    Segment segment(dir, 0, 7, 32, SegmentState::Sealed);
    EXPECT_TRUE(segment.isFull());
    EXPECT_EQ(segment.getPublishedOffset(), 7);
    EXPECT_TRUE(segment.getPublishedSize() > 0);
    result = segment.read(7, 256);
    EXPECT_EQ(result.last_read_offset, 7);
    ASSERT_EQ(result.result_buf.size(), 4 + SEGMENT_HEADER_SIZE);
    EXPECT_EQ(0, std::memcmp(datav[7].data(),
                             result.result_buf.data() + SEGMENT_HEADER_SIZE,
                             4 * sizeof(uint8_t)));

    result = segment.read(4, 256);
    EXPECT_EQ(result.last_read_offset, 7);
    ASSERT_EQ(result.result_buf.size(), 4 * (4 + SEGMENT_HEADER_SIZE));
    for (int i = 4; i < 8; ++i) {
        EXPECT_EQ(0,
                  std::memcmp(datav[i].data(),
                              result.result_buf.data() +
                                  (4 * sizeof(uint8_t) + SEGMENT_HEADER_SIZE) *
                                      (i - 4) +
                                  SEGMENT_HEADER_SIZE,
                              4 * sizeof(uint8_t)));
    }

    result = segment.read(8, 256);
    EXPECT_EQ(result.last_read_offset, 0);
    EXPECT_TRUE(result.result_buf.empty());
}

TEST_F(StorageEngineTests, IsFull) {
    std::filesystem::path dir = getDir() / "IsFull";
    Segment segment(dir, 0, SEGMENT_HEADER_SIZE + 1, SegmentState::Active);
    std::vector<uint8_t> data;
    data.push_back(1);
    uint64_t offset = segment.append(data.data(), sizeof(uint8_t));
    SegmentReadResult result = segment.read(0, 100);
    ASSERT_EQ(result.result_buf.size(), SEGMENT_HEADER_SIZE + 1);
    EXPECT_TRUE(segment.isFull());
    EXPECT_EQ(data[0], result.result_buf[SEGMENT_HEADER_SIZE]);
}

TEST_F(StorageEngineTests, LastReadOffset) {
    std::filesystem::path dir = getDir() / "last_read_offset";
    Segment segment(dir, 0, 4096, SegmentState::Active);
    std::vector<uint8_t> rec_payload{1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<Record> record_vec;

    for (unsigned int i = 0; i < 10; ++i) {
        auto record = RecordManager::create_record(rec_payload);
        record_vec.push_back(record);
    }
    size_t record_size = record_vec[0].to_bytes().size();
    std::vector<uint64_t> offsets;
    offsets.reserve(record_vec.size());
    for (auto &record : record_vec) {
        auto bytes = record.to_bytes();
        offsets.push_back(segment.append(bytes.data(), bytes.size()));
    }
    for (auto it = offsets.begin(); it != offsets.end(); ++it) {
        ASSERT_EQ(*it, it - offsets.begin());
    }

    SegmentReadResult result = segment.read(0, 5 * (record_size + 4));
    EXPECT_TRUE(result.result_buf.size() <= 5 * (record_size + 4));
    auto read_records = RecordManager::extract_records(result.result_buf);
    for (auto &record : read_records) {
        EXPECT_EQ(record.payload, rec_payload);
    }
    EXPECT_EQ(result.last_read_offset, 4);

    result = segment.read(2, 5 * (record_size + 4));
    EXPECT_TRUE(result.result_buf.size() <= 5 * (record_size + 4));
    read_records = RecordManager::extract_records(result.result_buf);
    for (auto &record : read_records) {
        EXPECT_EQ(record.payload, rec_payload);
    }
    EXPECT_EQ(result.last_read_offset, 6);
    result = segment.read(0, 4096);
    EXPECT_EQ(result.last_read_offset, 9);
}

TEST_F(StorageEngineTests, LogReadWrite) {
    std::filesystem::path dir = getDir() / "LogReadWrite";
    Log log(dir, SEGMENT_HEADER_SIZE + 1);
    log.start();

    std::vector<uint64_t> offsets;
    AppendData append_data;
    append_data.data.push_back(2);
    auto offset = log.append(append_data);
    FetchData fetch_data;
    fetch_data.offset = offset;
    fetch_data.max_bytes = 2 * (SEGMENT_HEADER_SIZE + 1);
    auto result = log.fetch(fetch_data);
    ASSERT_EQ(result.result_buf.size(), SEGMENT_HEADER_SIZE + 1);
    ASSERT_EQ(result.result_buf[SEGMENT_HEADER_SIZE], 2);
}

TEST_F(StorageEngineTests, LogReadWriteRollover) {
    std::filesystem::path dir = getDir() / "LogReadWriteRollover";
    Log log(dir, 4 * (SEGMENT_HEADER_SIZE + 1));
    log.start();

    std::vector<uint64_t> offsets;
    AppendData append_data;
    int i;
    for (i = 0; i < 98; ++i) {
        append_data.data.clear();
        append_data.data.push_back(i);
        auto offset = log.append(append_data);
        offsets.push_back(offset);
        ASSERT_EQ(offset, i);
    }

    FetchData fetch_data;
    for (i = 0; i < 98; ++i) {
        fetch_data.offset = i;
        fetch_data.max_bytes = 100 * (SEGMENT_HEADER_SIZE + 1);
        auto result = log.fetch(fetch_data);
        ASSERT_EQ(result.result_buf.size(),
                  (98 - i) * (SEGMENT_HEADER_SIZE + 1));
        for (int j = 0; j < 98 - i; ++j) {
            ASSERT_EQ(
                result.result_buf[(j + 1) * (SEGMENT_HEADER_SIZE + 1) - 1],
                i + j);
        }
    }
}

TEST_F(StorageEngineTests, LogReadWriteNonActive) {
    std::filesystem::path dir = getDir() / "LogReadNonActive";
    Log log(dir, 4 * (SEGMENT_HEADER_SIZE + 1));
    EXPECT_ANY_THROW(log.append({}));
    EXPECT_ANY_THROW(log.fetch({0, 32}));
}

using crc32c_type =
    boost::crc_optimal<32, 0x1EDC6F41, 0xFFFFFFFF, 0xFFFFFFFF, true, true>;

TEST_F(StorageEngineTests, LogCleanRecovery) {
    std::filesystem::path dir = getDir() / "LogCleanRecovery";
    crc32c_type crc32c;
    std::vector<uint32_t> checksums;
    {
        Log log(dir, 4 * (SEGMENT_HEADER_SIZE + 1));
        log.start();
        std::vector<uint64_t> offsets;
        AppendData append_data;
        for (uint8_t i = 0; i < 98; ++i) {
            crc32c.process_byte(i);
            uint32_t checksum = crc32c.checksum();
            append_data.data.clear();
            append_data.data.resize(sizeof(uint32_t));
            if (is_big_endian())
                byteswap32(checksum);
            checksums.push_back(checksum);
            std::memcpy(append_data.data.data(), &checksum, sizeof(uint32_t));
            append_data.data.push_back(i);
            auto offset = log.append(append_data);
            offsets.push_back(offset);
            ASSERT_EQ(offset, i);
            crc32c.reset();
        }
    }

    Log log(dir, 4 * (SEGMENT_HEADER_SIZE + 1));
    log.start();
    FetchData fetch_data;
    uint32_t checksum;
    for (uint8_t i = 0; i < 98; ++i) {
        fetch_data.offset = i;
        fetch_data.max_bytes = 500 * (SEGMENT_HEADER_SIZE + 1);
        auto result = log.fetch(fetch_data);
        ASSERT_EQ(result.result_buf.size(),
                  (98 - i) * (SEGMENT_HEADER_SIZE + 1 + sizeof(uint32_t)));
        for (int j = 0; j < 98 - i; ++j) {
            std::memcpy(&checksum,
                        result.result_buf.data() +
                            j * (SEGMENT_HEADER_SIZE + 1 + sizeof(uint32_t)) +
                            SEGMENT_HEADER_SIZE,
                        sizeof(uint32_t));
            if (is_big_endian())
                byteswap32(checksum);
            ASSERT_EQ(checksum, checksums[i + j]);
            ASSERT_EQ(result.result_buf[(j + 1) * (SEGMENT_HEADER_SIZE + 1 +
                                                   sizeof(uint32_t)) -
                                        1],
                      i + j);
        }
    }
}

TEST_F(StorageEngineTests, LogRecoveryEmptySegment) {
    std::filesystem::path dir = getDir() / "LogRecoveryEmptySegment";
    crc32c_type crc32c;
    std::vector<uint32_t> checksums;
    {
        Log log(dir, 500);
        log.start();
        std::vector<uint64_t> offsets;
        AppendData append_data;
        for (uint8_t i = 0; i < 3; ++i) {
            crc32c.process_byte(i);
            uint32_t checksum = crc32c.checksum();
            append_data.data.clear();
            append_data.data.resize(sizeof(uint32_t));
            if (is_big_endian())
                byteswap32(checksum);
            checksums.push_back(checksum);
            std::memcpy(append_data.data.data(), &checksum, sizeof(uint32_t));
            append_data.data.push_back(i);
            auto offset = log.append(append_data);
            offsets.push_back(offset);
            ASSERT_EQ(offset, i);
            crc32c.reset();
        }
    }

    std::string filename(64, '0');
    filename += ".log";
    auto log_file = dir / filename;
    std::filesystem::resize_file(log_file, 0);
    Log log(dir, 500);
    log.start();
    AppendData append_data;
    append_data.data.push_back(5);
    auto offset = log.append(append_data);
    ASSERT_EQ(offset, 0);
    FetchData req{0, 50};
    auto result = log.fetch(req);
    ASSERT_EQ(result.result_buf.size(), SEGMENT_HEADER_SIZE + 1);
    ASSERT_EQ(result.result_buf[result.result_buf.size() - 1], 5);
}

TEST_F(StorageEngineTests, LogTruncateMidRecord) {
    std::filesystem::path dir = getDir() / "LogTruncateMidRecord";
    crc32c_type crc32c;
    std::vector<uint32_t> checksums;
    std::vector<uint64_t> offsets;
    std::vector<uint8_t> buf;
    {
        Log log(dir, 1024);
        log.start();
        AppendData append_data;
        std::vector<uint8_t> buf;
        for (int i = 0; i < 4; ++i) {
            append_data.data.clear();
            buf.clear();
            append_data.data.resize(sizeof(uint32_t));
            for (uint8_t j = 0; j < 10; ++j) {
                buf.push_back((j * j) % i);
            }
            crc32c.process_bytes(buf.data(), buf.size());
            uint32_t checksum = crc32c.checksum();
            if (is_big_endian())
                byteswap32(checksum);
            checksums.push_back(checksum);
            std::memcpy(append_data.data.data(), &checksum, sizeof(uint32_t));
            for (const auto &byte : buf) {
                append_data.data.push_back(byte);
            }
            auto offset = log.append(append_data);
            offsets.push_back(offset);
            ASSERT_EQ(offset, i);
            crc32c.reset();
        }
    }

    std::string filename(64, '0');
    filename += ".log";
    auto log_file = dir / filename;
    std::filesystem::resize_file(log_file,
                                 std::filesystem::file_size(log_file) - 5);
    Log log(dir, 1024);
    log.start();

    ASSERT_EQ(log.getPublishedOffset(), offsets[offsets.size() - 2]);
    FetchData fetch_data;
    uint32_t checksum;
    fetch_data.offset = 0;
    fetch_data.max_bytes = 500;
    auto result = log.fetch(fetch_data);
    ASSERT_EQ(result.result_buf.size(),
              3 * (SEGMENT_HEADER_SIZE + 10 + sizeof(uint32_t)));
    for (int i = 0; i < 3; ++i) {
        std::memcpy(&checksum,
                    result.result_buf.data() +
                        i * (SEGMENT_HEADER_SIZE + 10 + sizeof(uint32_t)) +
                        SEGMENT_HEADER_SIZE,
                    sizeof(uint32_t));
        if (is_big_endian())
            byteswap32(checksum);
        ASSERT_EQ(checksum, checksums[i]);
        for (uint8_t j = 0; j < 10; ++j) {
            ASSERT_EQ(result.result_buf[(i + 1) * (SEGMENT_HEADER_SIZE +
                                                   sizeof(uint32_t) + 10) +
                                        j - 10],
                      (j * j) % i);
        }
    }
}

TEST_F(StorageEngineTests, LogTruncateRecordBoundary) {
    std::filesystem::path dir = getDir() / "LogTruncateRecordBoundary";
    crc32c_type crc32c;
    std::vector<uint32_t> checksums;
    std::vector<uint64_t> offsets;
    std::vector<uint8_t> buf;
    uint64_t truncate_pos = 0;
    {
        Log log(dir, 1024);
        log.start();
        AppendData append_data;
        std::vector<uint8_t> buf;
        for (int i = 0; i < 4; ++i) {
            append_data.data.clear();
            buf.clear();
            append_data.data.resize(sizeof(uint32_t));
            for (uint8_t j = 0; j < 10; ++j) {
                buf.push_back((j * j) % i);
            }
            crc32c.process_bytes(buf.data(), buf.size());
            uint32_t checksum = crc32c.checksum();
            if (is_big_endian())
                byteswap32(checksum);
            checksums.push_back(checksum);
            std::memcpy(append_data.data.data(), &checksum, sizeof(uint32_t));
            for (const auto &byte : buf) {
                append_data.data.push_back(byte);
            }
            auto offset = log.append(append_data);
            if (i != 3)
                truncate_pos += (append_data.data.size() + SEGMENT_HEADER_SIZE);
            offsets.push_back(offset);
            ASSERT_EQ(offset, i);
            crc32c.reset();
        }
    }

    std::string filename(64, '0');
    filename += ".log";
    auto log_file = dir / filename;
    std::filesystem::resize_file(log_file, truncate_pos);
    Log log(dir, 1024);
    log.start();

    ASSERT_EQ(log.getPublishedOffset(), offsets[offsets.size() - 2]);
    FetchData fetch_data;
    uint32_t checksum;
    fetch_data.offset = 0;
    fetch_data.max_bytes = 500;
    auto result = log.fetch(fetch_data);
    ASSERT_EQ(result.result_buf.size(),
              3 * (SEGMENT_HEADER_SIZE + 10 + sizeof(uint32_t)));
    for (int i = 0; i < 3; ++i) {
        std::memcpy(&checksum,
                    result.result_buf.data() +
                        i * (SEGMENT_HEADER_SIZE + 10 + sizeof(uint32_t)) +
                        SEGMENT_HEADER_SIZE,
                    sizeof(uint32_t));
        if (is_big_endian())
            byteswap32(checksum);
        ASSERT_EQ(checksum, checksums[i]);
        for (uint8_t j = 0; j < 10; ++j) {
            ASSERT_EQ(result.result_buf[(i + 1) * (SEGMENT_HEADER_SIZE +
                                                   sizeof(uint32_t) + 10) +
                                        j - 10],
                      (j * j) % i);
        }
    }
}

TEST_F(StorageEngineTests, LogRecoveryNoIndex) {
    std::filesystem::path dir = getDir() / "LogRecoveryNoIndex";
    crc32c_type crc32c;
    std::vector<uint32_t> checksums;
    std::vector<uint64_t> offsets;
    std::vector<uint8_t> buf;
    uint64_t truncate_pos = 0;
    {
        Log log(dir, 1024);
        log.start();
        AppendData append_data;
        std::vector<uint8_t> buf;
        for (int i = 0; i < 4; ++i) {
            append_data.data.clear();
            buf.clear();
            append_data.data.resize(sizeof(uint32_t));
            for (uint8_t j = 0; j < 10; ++j) {
                buf.push_back((j * j) % i);
            }
            crc32c.process_bytes(buf.data(), buf.size());
            uint32_t checksum = crc32c.checksum();
            if (is_big_endian())
                byteswap32(checksum);
            checksums.push_back(checksum);
            std::memcpy(append_data.data.data(), &checksum, sizeof(uint32_t));
            for (const auto &byte : buf) {
                append_data.data.push_back(byte);
            }
            auto offset = log.append(append_data);
            offsets.push_back(offset);
            ASSERT_EQ(offset, i);
            crc32c.reset();
        }
    }

    std::string filename(64, '0');
    filename += ".index";
    auto index_file = dir / filename;
    ASSERT_TRUE(std::filesystem::remove(index_file));
    Log log(dir, 1024);
    log.start();

    ASSERT_EQ(log.getPublishedOffset(), offsets[offsets.size() - 1]);
    FetchData fetch_data;
    uint32_t checksum;
    fetch_data.offset = 0;
    fetch_data.max_bytes = 500;
    auto result = log.fetch(fetch_data);
    ASSERT_EQ(result.result_buf.size(),
              4 * (SEGMENT_HEADER_SIZE + 10 + sizeof(uint32_t)));
    for (int i = 0; i < 4; ++i) {
        std::memcpy(&checksum,
                    result.result_buf.data() +
                        i * (SEGMENT_HEADER_SIZE + 10 + sizeof(uint32_t)) +
                        SEGMENT_HEADER_SIZE,
                    sizeof(uint32_t));
        if (is_big_endian())
            byteswap32(checksum);
        ASSERT_EQ(checksum, checksums[i]);
        for (uint8_t j = 0; j < 10; ++j) {
            ASSERT_EQ(result.result_buf[(i + 1) * (SEGMENT_HEADER_SIZE +
                                                   sizeof(uint32_t) + 10) +
                                        j - 10],
                      (j * j) % i);
        }
    }
}

std::vector<uint64_t> getSortedBaseOffsets(const std::filesystem::path &dir) {
    std::vector<uint64_t> base_offsets;
    std::string filename;
    for (const auto &entry : std::filesystem::directory_iterator(dir)) {
        filename = entry.path().filename().string();
        if (filename.compare(filename.size() - 4, 4, ".log") == 0)
            base_offsets.push_back(std::stoll(
                filename.substr(0, filename.size() - 4), nullptr, 10));
    }
    std::sort(base_offsets.begin(), base_offsets.end());
    return base_offsets;
}

TEST_F(StorageEngineTests, LogRecoveryAfterRolloverFsync) {
    std::filesystem::path dir = getDir() / "LogRecoveryAfterRolloverFsync";
    crc32c_type crc32c;
    std::vector<uint32_t> checksums;
    std::vector<uint64_t> offsets, base_offsets;
    std::vector<uint8_t> buf;
    uint64_t truncate_pos = 0;
    {
        Log log(dir, 32);
        log.start();
        AppendData append_data;
        std::vector<uint8_t> buf;
        int i = 0;
        while (base_offsets.size() < 2) {
            append_data.data.clear();
            buf.clear();
            append_data.data.resize(sizeof(uint32_t));
            for (uint8_t j = 0; j < 10; ++j) {
                buf.push_back((j * j) % i);
            }
            crc32c.process_bytes(buf.data(), buf.size());
            uint32_t checksum = crc32c.checksum();
            if (is_big_endian())
                byteswap32(checksum);
            checksums.push_back(checksum);
            std::memcpy(append_data.data.data(), &checksum, sizeof(uint32_t));
            for (const auto &byte : buf) {
                append_data.data.push_back(byte);
            }
            auto offset = log.append(append_data);
            offsets.push_back(offset);
            ASSERT_EQ(offset, i);
            crc32c.reset();
            ++i;
            base_offsets = getSortedBaseOffsets(dir);
        }
    }

    // delete new segment file
    auto filename = std::to_string(base_offsets[1]) + ".log";
    std::string filler(68 - filename.size(), '0');
    filename = filler + filename;
    auto log_file = dir / filename;
    std::filesystem::resize_file(log_file, 0);
    Log log(dir, 1024);
    log.start();

    ASSERT_EQ(getSortedBaseOffsets(dir).size(), 2);
    ASSERT_TRUE(std::filesystem::is_empty(log_file));
    ASSERT_EQ(log.getPublishedOffset(), offsets[offsets.size() - 2]);
    FetchData fetch_data;
    uint32_t checksum;
    fetch_data.offset = 0;
    fetch_data.max_bytes = 500;
    auto result = log.fetch(fetch_data);
    ASSERT_EQ(result.result_buf.size(),
              (offsets.size() - 1) *
                  (SEGMENT_HEADER_SIZE + 10 + sizeof(uint32_t)));
    for (int i = 0; i < offsets.size() - 1; ++i) {
        std::memcpy(&checksum,
                    result.result_buf.data() +
                        i * (SEGMENT_HEADER_SIZE + 10 + sizeof(uint32_t)) +
                        SEGMENT_HEADER_SIZE,
                    sizeof(uint32_t));
        if (is_big_endian())
            byteswap32(checksum);
        ASSERT_EQ(checksum, checksums[i]);
        for (uint8_t j = 0; j < 10; ++j) {
            ASSERT_EQ(result.result_buf[(i + 1) * (SEGMENT_HEADER_SIZE +
                                                   sizeof(uint32_t) + 10) +
                                        j - 10],
                      (j * j) % i);
        }
    }
}

TEST_F(StorageEngineTests, LogRolloverTruncateActiveSegment) {
    std::filesystem::path dir = getDir() / "LogRolloverTruncateActiveSegment";
    crc32c_type crc32c;
    std::vector<uint32_t> checksums;
    std::vector<uint64_t> offsets, base_offsets;
    std::vector<uint8_t> buf;
    uint64_t truncate_pos = 0;
    {
        Log log(dir, 32);
        log.start();
        AppendData append_data;
        std::vector<uint8_t> buf;
        int i = 0;
        while (base_offsets.size() < 2) {
            append_data.data.clear();
            buf.clear();
            append_data.data.resize(sizeof(uint32_t));
            for (uint8_t j = 0; j < 10; ++j) {
                buf.push_back((j * j) % i);
            }
            crc32c.process_bytes(buf.data(), buf.size());
            uint32_t checksum = crc32c.checksum();
            if (is_big_endian())
                byteswap32(checksum);
            checksums.push_back(checksum);
            std::memcpy(append_data.data.data(), &checksum, sizeof(uint32_t));
            for (const auto &byte : buf) {
                append_data.data.push_back(byte);
            }
            auto offset = log.append(append_data);
            offsets.push_back(offset);
            ASSERT_EQ(offset, i);
            crc32c.reset();
            ++i;
            base_offsets = getSortedBaseOffsets(dir);
        }
    }

    // truncate new segment file
    auto filename = std::to_string(base_offsets[1]) + ".log";
    std::string filler(68 - filename.size(), '0');
    filename = filler + filename;
    auto log_file = dir / filename;
    std::filesystem::resize_file(log_file,
                                 std::filesystem::file_size(log_file) - 5);
    Log log(dir, 1024);
    log.start();
    ASSERT_EQ(getSortedBaseOffsets(dir).size(), 2);
    ASSERT_TRUE(std::filesystem::is_empty(log_file));

    ASSERT_EQ(log.getPublishedOffset(), offsets[offsets.size() - 2]);
    FetchData fetch_data;
    uint32_t checksum;
    fetch_data.offset = 0;
    fetch_data.max_bytes = 500;
    auto result = log.fetch(fetch_data);
    ASSERT_EQ(result.result_buf.size(),
              (offsets.size() - 1) *
                  (SEGMENT_HEADER_SIZE + 10 + sizeof(uint32_t)));
    for (int i = 0; i < offsets.size() - 1; ++i) {
        std::memcpy(&checksum,
                    result.result_buf.data() +
                        i * (SEGMENT_HEADER_SIZE + 10 + sizeof(uint32_t)) +
                        SEGMENT_HEADER_SIZE,
                    sizeof(uint32_t));
        if (is_big_endian())
            byteswap32(checksum);
        ASSERT_EQ(checksum, checksums[i]);
        for (uint8_t j = 0; j < 10; ++j) {
            ASSERT_EQ(result.result_buf[(i + 1) * (SEGMENT_HEADER_SIZE +
                                                   sizeof(uint32_t) + 10) +
                                        j - 10],
                      (j * j) % i);
        }
    }
}

/*
    Crash recovery tests
    1. Delete index file -> done
    2. Corrupt index file
    3. Crash involving rollover -> done
*/

/*
    Index tests
    1. sparse and non-sparse -> done
    2. Active and sealed -> done
    3. Adding IndexEntry is monotonic w.r.t. offset, i.e. offset of new
   entry should be strictly larger than current largest indexed offset (Also
       prevents duplicate indexing) -> done
    4. Empty index -> done
    5. Large index/offset?
*/

TEST_F(StorageEngineTests, IndexRW) {
    std::filesystem::path dir = getDir() / "IndexRW";
    {
        Index index(dir, 0, SegmentState::Active);
        std::optional<IndexFileEntry> entry_opt;
        IndexFileEntry entry;
        uint32_t file_pos = 0;
        for (uint64_t i = 0; i < 1000; ++i) {
            entry.offset = i;
            entry.file_position = file_pos;
            index.append(entry);
            file_pos += 25 * (i % 4 + 1);
        }
        file_pos = 0;
        for (uint64_t i = 0; i < 1000; ++i) {
            entry_opt = index.determineClosestIndex(i);
            ASSERT_TRUE(entry_opt.has_value());
            EXPECT_EQ(entry_opt.value().offset, i);
            EXPECT_EQ(entry_opt.value().file_position, file_pos);
            file_pos += 25 * (i % 4 + 1);
        }
    }
    Index index(dir, 0, SegmentState::Sealed);
    IndexFileEntry entry;
    std::optional<IndexFileEntry> entry_opt;
    uint32_t file_pos = 0;
    for (uint64_t i = 0; i < 10; ++i) {
        entry_opt = index.determineClosestIndex(i);
        ASSERT_TRUE(entry_opt.has_value());
        EXPECT_EQ(entry_opt.value().offset, i);
        EXPECT_EQ(entry_opt.value().file_position, file_pos);
        file_pos += 25 * (i % 4 + 1);
    }
}

TEST_F(StorageEngineTests, IndexRWSparse) {
    std::filesystem::path dir = getDir() / "IndexRWSparse";
    {
        Index index(dir, 0, SegmentState::Active);
        IndexFileEntry entry;
        std::optional<IndexFileEntry> entry_opt;
        uint32_t file_pos = 0;
        for (uint64_t i = 2; i < 1000; ++i) {
            entry.offset = i * i;
            entry.file_position = file_pos;
            index.append(entry);
            file_pos += 25 * (i % 4 + 1);
        }
        file_pos = 0;
        for (uint64_t i = 2; i < 1000; ++i) {
            entry_opt = index.determineClosestIndex(i * i + i);
            ASSERT_TRUE(entry_opt.has_value());
            EXPECT_EQ(entry_opt.value().offset, i * i);
            EXPECT_EQ(entry_opt.value().file_position, file_pos);
            file_pos += 25 * (i % 4 + 1);
        }
    }
    Index index(dir, 0, SegmentState::Sealed);
    IndexFileEntry entry;
    std::optional<IndexFileEntry> entry_opt;
    uint32_t file_pos = 0;
    for (uint64_t i = 2; i < 1000; ++i) {
        entry_opt = index.determineClosestIndex(i * i + i);
        ASSERT_TRUE(entry_opt.has_value());
        EXPECT_EQ(entry_opt.value().offset, i * i);
        EXPECT_EQ(entry_opt.value().file_position, file_pos);
        file_pos += 25 * (i % 4 + 1);
    }
}

TEST_F(StorageEngineTests, IndexMonotonic) {
    std::filesystem::path dir = getDir() / "IndexMonotonic";
    {
        Index index(dir, 0, SegmentState::Active);
        IndexFileEntry entry;
        uint32_t file_pos = 0;
        for (uint64_t i = 0; i < 10; ++i) {
            entry.offset = i;
            entry.file_position = file_pos;
            index.append(entry);
            file_pos += 25 * (i % 4 + 1);
        }
        entry.offset = 5;
        entry.file_position = 25;
        EXPECT_ANY_THROW(index.append(entry));
    }
    // Sealed index does not allow append
    Index index(dir, 0, SegmentState::Sealed);
    IndexFileEntry entry{11, 1500};
    EXPECT_ANY_THROW(index.append(entry));
}

TEST_F(StorageEngineTests, IndexFindWhenEmpty) {
    std::filesystem::path dir = getDir() / "IndexFindWhenEmpty";
    Index index(dir, 0, SegmentState::Active);
    auto entry = index.determineClosestIndex(0);
    ASSERT_FALSE(entry.has_value());
}

TEST_F(StorageEngineTests, LogRwLarge) {
    std::filesystem::path dir = getDir() / "log_rw_large";
    Log log(dir, 2048);
    log.start();

    auto records = generate_records(100, 8000);
    for (auto &record : records) {
        log.append({record.to_bytes()});
    }
    auto fetch_result = log.fetch({0, 1000000});
    auto fetched_records =
        RecordManager::extract_records(fetch_result.result_buf);
    ASSERT_EQ(records.size(), fetched_records.size());
    for (auto it = records.begin(); it != records.end(); ++it) {
        EXPECT_EQ(it->checksum, fetched_records[it - records.begin()].checksum);
        EXPECT_EQ(it->payload, fetched_records[it - records.begin()].payload);
    }
}

TEST_F(StorageEngineTests, SegmentReadOffsetInvalid) {
    std::filesystem::path dir = getDir() / "segment_read_offset_invalid";
    Segment segment(dir, 0, 4096, SegmentState::Active);
    auto records = generate_records(10, 100);
    for (auto &record : records) {
        segment.append(record.to_bytes().data(), record.to_bytes().size());
    }
    uint64_t offset = std::numeric_limits<uint64_t>::max();
    auto fetch_result = segment.read(offset, 4096);
    ASSERT_TRUE(fetch_result.result_buf.empty());
    offset = std::numeric_limits<uint64_t>::min() - 1;
    fetch_result = segment.read(offset, 4096);
    ASSERT_TRUE(fetch_result.result_buf.empty());
}

TEST_F(StorageEngineTests, SegmentReadMaxBytesInvalid) {
    std::filesystem::path dir = getDir() / "segment_read_offset_invalid";
    Segment segment(dir, 0, 4096, SegmentState::Active);
    auto records = generate_records(10, 100);
    for (auto &record : records) {
        segment.append(record.to_bytes().data(), record.to_bytes().size());
    }
    size_t max_bytes = std::numeric_limits<size_t>::max();
    EXPECT_ANY_THROW(segment.read(50, max_bytes));
    max_bytes = std::numeric_limits<size_t>::max() - 1;
    auto fetch_result = segment.read(50, max_bytes);
    auto fetched_records =
        RecordManager::extract_records(fetch_result.result_buf);
    EXPECT_EQ(fetched_records.size(), 50);
    max_bytes = std::numeric_limits<size_t>::min() - 1;
    EXPECT_ANY_THROW(segment.read(50, max_bytes));
    max_bytes = -1;
    EXPECT_ANY_THROW(segment.read(50, max_bytes));
}

} // namespace broker
} // namespace kafka_lite
