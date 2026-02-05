#include "../include/Segment.h"
#include <atomic>
#include <bit>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

constexpr bool is_big_endian() {
    return std::endian::native == std::endian::big;
}

constexpr std::uint32_t byteswap32(std::uint32_t value) {
    return ((value & 0x000000FF) << 24) | ((value & 0x0000FF00) << 8) |
           ((value & 0x00FF0000) >> 8) | ((value & 0xFF000000) >> 24);
}

constexpr std::uint64_t byteswap64(std::uint64_t value) {
    return ((value & 0x00000000000000FF) << 56) |
           ((value & 0x000000000000FF00) << 40) |
           ((value & 0x0000000000FF0000) << 24) |
           ((value & 0x00000000FF000000) << 8) |
           ((value & 0x000000FF00000000) >> 8) |
           ((value & 0x0000FF0000000000) >> 24) |
           ((value & 0x00FF000000000000) >> 40) |
           ((value & 0xFF00000000000000) >> 56);
}

bool write_u32_le(int fd, u_int32_t value) {
    if (is_big_endian())
        value = byteswap32(value);

    ssize_t curr_write, bytes_written = 0;
    while (bytes_written < sizeof(value)) {
        curr_write =
            write(fd, &value + bytes_written, sizeof(value) - bytes_written);
        if (curr_write < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (curr_write == 0)
            break;
        bytes_written += curr_write;
    }
    return bytes_written == sizeof(value);
}

bool write_u64_le(int fd, u_int64_t value) {
    if (is_big_endian())
        value = byteswap64(value);

    ssize_t curr_write, bytes_written = 0;
    while (bytes_written < sizeof(value)) {
        curr_write =
            write(fd, &value + bytes_written, sizeof(value) - bytes_written);
        if (curr_write < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (curr_write == 0)
            break;
        bytes_written += curr_write;
    }
    return bytes_written == sizeof(value);
}

namespace kafka_lite {
namespace broker {

Segment::Segment(const std::filesystem::path &dir, uint64_t base_offset,
                 uint64_t max_size, SegmentState state)
    : dir_(dir), base_offset_(base_offset), max_size_(max_size), log_fd_(-1),
      state_(state), published_size_(0), published_offset_(base_offset),
      index_file_(dir, base_offset, state) {
    init();
}

Segment::Segment(const std::filesystem::path &dir, uint64_t base_offset,
                 uint64_t published_offset, uint64_t max_size,
                 SegmentState state)
    : dir_(dir), base_offset_(base_offset), max_size_(max_size), log_fd_(-1),
      state_(state), published_size_(0), published_offset_(published_offset),
      index_file_(dir, base_offset, state) {
    init();
}

void Segment::init() {
    // maybe check if published offset < base offset and throw exception if true
    std::string filename = std::to_string(base_offset_) + ".log";
    std::filesystem::create_directories(dir_);
    std::filesystem::path log_file = dir_.append(filename);
    mode_t mode;
    int flags, rc;
    if (state_ == SegmentState::Active) {
        flags = O_RDWR | O_CREAT;
        mode = 0644;
    } else if (state_ == SegmentState::Sealed) {
        flags = O_RDONLY;
        mode = 0;
    }
    do {
        log_fd_ = open(log_file.c_str(), flags, mode);
    } while (log_fd_ == -1 && errno == EINTR);

    if (log_fd_ == -1)
        throw std::ios_base::failure("Failed to open log file.");

    struct stat st;
    do {
        rc = fstat(log_fd_, &st);
    } while (rc == -1 && errno == EINTR);
    if (rc == -1)
        throw std::runtime_error("Failure of fstat.");
    published_size_.store(st.st_size);
}

Segment::~Segment() {
    if (log_fd_ != 1)
        ::close(log_fd_);
}

FetchResult Segment::read(uint64_t offset, size_t max_bytes) {
    // use acquire semantics to ensure synchronization with append
    uint64_t current_offset = offset,
             pub_offset = published_offset_.load(std::memory_order_acquire),
             segment_size = published_size_.load(std::memory_order_acquire);

    if (offset > pub_offset ||
        published_size_.load(std::memory_order_acquire) == 0)
        return {}; // May need to be changed if I want to use sendfile() in the
                   // future

    FetchResult result;
    /*
        len: 32 bits
        checksum: 32 bits (once we include it)
        payload: variable length
    */
    uint32_t offset_file_position = determineFilePosition(offset);

    size_t len;
    if (segment_size - offset_file_position > max_bytes) {
        uint64_t current_offset = pub_offset;
        IndexFileEntry entry;
        do {
            entry = index_file_.determineClosestIndex(current_offset);
            current_offset = entry.offset - 1;
        } while (entry.file_position - offset_file_position > max_bytes);

        uint32_t next_file_position = determineFilePosition(entry.offset + 1),
                 current_file_position = entry.file_position;
        current_offset = entry.offset + 1;

        while (next_file_position - offset_file_position < max_bytes) {
            ++current_offset;
            current_file_position = next_file_position;
            next_file_position = determineFilePosition(current_offset, entry);
        }
        len = current_file_position - offset_file_position; // +1?
    } else
        len = segment_size - offset_file_position;

    // use pread for thread safety
    result.result_buf.resize(len);
    size_t curr_read, bytes_read = 0;
    while (bytes_read < len) {
        curr_read = pread(log_fd_, result.result_buf.data() + bytes_read,
                          len - bytes_read, offset_file_position + bytes_read);
        if (curr_read < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (curr_read == 0)
            break;
        bytes_read += curr_read;
    }

    if (bytes_read < len)
        throw std::ios_base::failure("Failed to read offset from log file.");

    if (state_ == SegmentState::Active)
        verifyDataIntegrity(result);

    return result;
}

uint64_t Segment::append(const uint8_t *data, uint32_t len) {
    // Write to log files, maybe need to handle write errors
    off_t pos = lseek(log_fd_, 0, SEEK_CUR);
    // lseek might fail, check error and reopen file if necessary or throw
    // exception

    if (pos > static_cast<off_t>(std::numeric_limits<uint32_t>::max())) {
        throw std::overflow_error("File position does not fit in uint32_t.");
    }

    if (!write_u32_le(log_fd_, len))
        throw std::ios_base::failure("Failed to write length to log file.");
    /*
        Future feature: Create and write checksum -> use crc32, there should be
       libraries that do this (boost::crc?)
    */

    uint64_t curr_write, bytes_written = 0;
    while (bytes_written < len) {
        curr_write = write(log_fd_, data, len - bytes_written);
        if (curr_write < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (curr_write == 0)
            break;
        bytes_written += curr_write;
        data += curr_write;
    }

    if (bytes_written < len)
        throw std::ios_base::failure("Failed to write data to log file.");

    // Increase published offset in thread safe way and write to index file
    uint64_t offset;
    if (published_size_.load(std::memory_order_acquire) == 0) {
        offset = base_offset_;
        published_offset_.store(offset, std::memory_order_release);
    } else
        offset = published_offset_.fetch_add(1, std::memory_order_release) + 1;
    IndexFileEntry index_data;
    index_data.offset = offset;
    index_data.file_position = static_cast<uint32_t>(pos);
    index_file_.append(index_data);

    uint64_t new_size = published_size_.fetch_add(
        bytes_written + SEGMENT_HEADER_SIZE, std::memory_order_release);
    return offset;
}

uint32_t Segment::determineFilePosition(uint64_t offset) {
    IndexFileEntry entry = index_file_.determineClosestIndex(offset);
    return determineFilePosition(offset, entry);
}

uint32_t Segment::determineFilePosition(uint64_t offset,
                                        const IndexFileEntry &entry) {
    uint64_t current_offset = entry.offset;
    uint32_t record_len, current_file_pos = entry.file_position;
    while (current_offset < offset) {
        ++current_offset;
        size_t curr_read, bytes_read = 0;
        while (bytes_read < FILE_POS_INDEX_SIZE) {
            curr_read = pread(log_fd_, &record_len + bytes_read,
                              FILE_POS_INDEX_SIZE - bytes_read,
                              current_file_pos + bytes_read);
            if (curr_read < 0) {
                if (errno == EINTR)
                    continue;
                break;
            }
            if (curr_read == 0)
                break;
            bytes_read += curr_read;
        }

        if (bytes_read != FILE_POS_INDEX_SIZE)
            throw std::ios_base::failure(
                "Failed to read length bytes from log file.");
        if (is_big_endian())
            record_len = byteswap32(record_len);
        current_file_pos += record_len + SEGMENT_HEADER_SIZE;
    }
    return current_file_pos;
}

void Segment::verifyDataIntegrity(FetchResult &result) {
    /*
        For later when we include the checksums.
        This will verify the integrity of a record.
        If a record is corrupted, then the result is truncated to hold all the
       records up until the corrupted one.
    */
}

Index::Index(const std::filesystem::path &dir, uint64_t base_offset,
             SegmentState state)
    : dir_(dir), published_size(0), fd_(-1), state_(state) {
    std::string filename = std::to_string(base_offset) + ".index";
    std::filesystem::create_directories(dir);
    std::filesystem::path log_file = dir_.append(filename);
    mode_t mode;
    int flags;
    if (state_ == SegmentState::Active) {
        flags = O_RDWR | O_CREAT;
        mode = 0644;
    } else if (state_ == SegmentState::Sealed) {
        flags = O_RDONLY;
        mode = 0;
    }
    do {
        fd_ = open(log_file.c_str(), flags, mode);
    } while (fd_ == -1 && errno == EINTR);

    if (fd_ == -1)
        throw std::ios_base::failure("Failed to open index file.");

    if (state_ == SegmentState::Sealed) {
        struct stat st;
        int rc;
        do {
            rc = fstat(fd_, &st);
        } while (rc == -1 && errno == EINTR);
		// What if rc signals an error? I think also then an exception should be thrown
        void *mrc = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd_, 0);
        if (mrc == MAP_FAILED)
            throw ::std::runtime_error("Failure of mmap.");
        mmap_base_offset_ = reinterpret_cast<const char *>(mrc);
        published_size.store(st.st_size, std::memory_order_release);
        close(fd_);
    }
}

Index::~Index() {
    if (state_ == SegmentState::Sealed) {
        size_t size = published_size.load(std::memory_order_acquire);
        munmap(const_cast<char *>(mmap_base_offset_), size);
    }
    if (fd_ != -1)
        close(fd_);
}

IndexFileEntry Index::determineClosestIndex(uint64_t offset) {
    uint64_t file_size = published_size.load(std::memory_order_acquire);
    if (state_ == SegmentState::Active)
        return binarySearch(
            offset, reinterpret_cast<const char *>(data_.data()), file_size);
    return binarySearch(offset, mmap_base_offset_, file_size);
}

void Index::append(const IndexFileEntry &data) {
    if (!write_u64_le(fd_, data.offset))
        throw std::ios_base::failure("Failed to write offset to index file.");
    data_.resize(data_.size() + INDEX_ENTRY_SIZE);
    uint64_t value = data.offset;
    if (is_big_endian())
        value = byteswap64(value);
    std::memcpy(data_.data() + data_.size() - INDEX_ENTRY_SIZE, &value,
                OFFSET_SIZE);

    if (!write_u32_le(fd_, data.file_position))
        throw std::ios_base::failure(
            "Failed to write file position to index file.");
    uint32_t fpos_index = data.file_position;
    if (is_big_endian())
        fpos_index = byteswap32(fpos_index);
    std::memcpy(data_.data() + data_.size() - FILE_POS_INDEX_SIZE, &fpos_index,
                FILE_POS_INDEX_SIZE);

    uint64_t size =
        published_size.fetch_add(INDEX_ENTRY_SIZE, std::memory_order_release);
}

IndexFileEntry Index::binarySearch(uint64_t offset, const char *buf,
                                   uint64_t file_size) {
    // Entries have 12 bytes, 8 for the offset and 4 for the file position
    IndexFileEntry entry;
    uint64_t current_offset = 0, L_pos = 0,
             R_pos = file_size - INDEX_ENTRY_SIZE,
             diff = file_size / INDEX_ENTRY_SIZE - 1;
    const char *L = buf;
    const char *R = L + file_size - INDEX_ENTRY_SIZE;

    // Check if the offset we are seeking is indexed at the start or end
    std::memcpy(&current_offset, L, OFFSET_SIZE);
    if (is_big_endian())
        current_offset = byteswap64(current_offset);
    if (current_offset == offset) {
        entry.offset = current_offset;
        std::memcpy(&entry.file_position, L + OFFSET_SIZE,
                    sizeof(entry.file_position));
        if (is_big_endian())
            entry.file_position = byteswap32(entry.file_position);
        return entry;
    }

    std::memcpy(&current_offset, R, sizeof(current_offset));
    if (is_big_endian())
        current_offset = byteswap64(current_offset);
    if (current_offset == offset) {
        entry.offset = current_offset;
        std::memcpy(&entry.file_position, R + OFFSET_SIZE,
                    sizeof(entry.file_position));
        if (is_big_endian())
            entry.file_position = byteswap32(entry.file_position);
        return entry;
    }

    // If not, do binary search
    const char *M;
    while (L < R) {
        M = L + INDEX_ENTRY_SIZE * (diff / 2+1);
        std::memcpy(&current_offset, M, sizeof(current_offset));
        if (is_big_endian())
            current_offset = byteswap64(current_offset);
        if (current_offset <= offset) {
            L = M;
            diff -= (diff / 2+1);
        } else {
            R = M-INDEX_ENTRY_SIZE;
            diff /= 2;
        }
    }

    std::memcpy(&entry.offset, L, sizeof(entry.offset));
    std::memcpy(&entry.file_position, L + OFFSET_SIZE, FILE_POS_INDEX_SIZE);
    if (is_big_endian()) {
        entry.offset = byteswap64(entry.offset);
        entry.file_position = byteswap32(entry.file_position);
        }
    return entry;
}

bool Segment::isFull() {
    uint64_t size = published_size_.load(std::memory_order_acquire);
    return (size >= max_size_);
}
} // namespace broker
} // namespace kafka_lite
