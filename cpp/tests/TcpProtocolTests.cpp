#include "../include/TcpProtocol.h"
#include <cstdint>
#include <cstring>
#include <gtest/gtest.h>
#include <variant>
#include <vector>

namespace kafka_lite {
namespace broker {

class TcpProtocolTests : public ::testing::Test {};

/*
1. parse length
2. parse magic bytes -> drop request if not correct
3. parse headers
    1. What can go wrong here, i.e. what needs to be validated?
        1. Wrong version
        2. Unknown type
        3. Unknown flags
4. read payload length
5. parse tcp request -> Instantiate TcpRequest and call
TcpRequest::to_specialized_type()
    1. This does not need validation, client is responsible for ensuring payload
integrity
*/

TEST(TcpProtocolTests, header_append_request) {
    boost::uuids::uuid correlation_id = {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad,
                                          0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0,
                                          0x4f, 0xd4, 0x30, 0xc8}};
    TcpHeaders header_write{correlation_id, 0, RequestType::Append, 0};
    auto bytes = header_write.to_bytes();
    TcpHeaders header_read;
    ASSERT_TRUE(header_read.from_bytes(bytes));
    EXPECT_EQ(header_read.correlation_id, header_write.correlation_id);
    EXPECT_EQ(header_read.flags, header_write.flags);
    EXPECT_EQ(header_read.protocol_version, header_write.protocol_version);
    EXPECT_EQ(header_read.type, header_write.type);
    EXPECT_EQ(header_read.getParseError(), ParseError::NO_ERROR);
}

TEST(TcpProtocolTests, header_fetch_request) {
    boost::uuids::uuid correlation_id = {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad,
                                          0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0,
                                          0x4f, 0xd4, 0x30, 0xc8}};
    TcpHeaders header_write{correlation_id, 0, RequestType::Fetch, 0};
    auto bytes = header_write.to_bytes();
    TcpHeaders header_read;
    ASSERT_TRUE(header_read.from_bytes(bytes));
    EXPECT_EQ(header_read.correlation_id, header_write.correlation_id);
    EXPECT_EQ(header_read.flags, header_write.flags);
    EXPECT_EQ(header_read.protocol_version, header_write.protocol_version);
    EXPECT_EQ(header_read.type, header_write.type);
    EXPECT_EQ(header_read.getParseError(), ParseError::NO_ERROR);
}

TEST(TcpProtocolTests, header_wrong_protocol_version) {
    boost::uuids::uuid correlation_id = {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad,
                                          0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0,
                                          0x4f, 0xd4, 0x30, 0xc8}};
    TcpHeaders header_write{correlation_id, 1, RequestType::Append, 0};
    auto bytes = header_write.to_bytes();
    TcpHeaders header_read;
    ASSERT_FALSE(header_read.from_bytes(bytes));
    EXPECT_EQ(header_read.getParseError(), ParseError::ERR_UNSUPPORTED_VERSION);
}

TEST(TcpProtocolTests, header_unknown_type) {
    boost::uuids::uuid correlation_id = {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad,
                                          0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0,
                                          0x4f, 0xd4, 0x30, 0xc8}};
    TcpHeaders header_write{correlation_id, 0, RequestType::Append, 0};
    auto bytes = header_write.to_bytes();
    ASSERT_TRUE(bytes.size() > correlation_id.size() + 1);
    bytes[correlation_id.size() + 1] = -1;
    TcpHeaders header_read;
    ASSERT_FALSE(header_read.from_bytes(bytes));
    EXPECT_EQ(header_read.getParseError(), ParseError::ERR_UNKNOWN_TYPE);
}

TEST(TcpProtocolTests, header_unknown_flags) {
    boost::uuids::uuid correlation_id = {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad,
                                          0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0,
                                          0x4f, 0xd4, 0x30, 0xc8}};
    TcpHeaders header_write{correlation_id, 0, RequestType::Append, 1};
    auto bytes = header_write.to_bytes();
    ASSERT_TRUE(bytes.size() > correlation_id.size() + 1);
    TcpHeaders header_read;
    ASSERT_FALSE(header_read.from_bytes(bytes));
    EXPECT_EQ(header_read.getParseError(), ParseError::ERR_UNSUPPORTED_FLAGS);
}

TEST(TcpProtocolTests, header_missing_correlation_id) {
    boost::uuids::uuid correlation_id = {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad,
                                          0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0,
                                          0x4f, 0xd4, 0x30, 0xc8}};
    TcpHeaders header_write{correlation_id, 0, RequestType::Append, 1};
    auto bytes = header_write.to_bytes();
    std::vector<uint8_t> truncated_bytes(bytes.size() - correlation_id.size());
    std::memcpy(truncated_bytes.data(), bytes.data() + correlation_id.size(),
                truncated_bytes.size());
    TcpHeaders header_read;
    ASSERT_FALSE(header_read.from_bytes(truncated_bytes));
    EXPECT_EQ(header_read.getParseError(),
              ParseError::ERR_MISSING_CORRELATION_ID);
}

TEST(TcpProtocolTests, tcp_request_to_append_request) {
    boost::uuids::uuid correlation_id = {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad,
                                          0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0,
                                          0x4f, 0xd4, 0x30, 0xc8}};
    TcpHeaders header{correlation_id, 0, RequestType::Append, 0};
    std::vector<uint8_t> payload{0, 1, 5, 6, 8};
    TcpRequest request{.headers = header, .payload = payload};
    auto alternative = request.to_specialized_type();
    ASSERT_TRUE(std::holds_alternative<AppendRequest>(alternative));
    auto append_request = std::get<AppendRequest>(alternative);
    ASSERT_EQ(append_request.correlation_id, correlation_id);
    ASSERT_EQ(append_request.payload, payload);
}

TEST(TcpProtocolTests, tcp_request_to_fetch_request) {
    boost::uuids::uuid correlation_id = {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad,
                                          0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0,
                                          0x4f, 0xd4, 0x30, 0xc8}};
    TcpHeaders header{correlation_id, 0, RequestType::Fetch, 0};
    uint64_t offset = 200;
    uint32_t max_bytes = 1024;
    auto payload = TcpRequest::make_payload(offset, max_bytes);
    TcpRequest request{.headers = header, .payload = payload};
    auto alternative = request.to_specialized_type();
    ASSERT_TRUE(std::holds_alternative<FetchRequest>(alternative));
    auto fetch_request = std::get<FetchRequest>(alternative);
    ASSERT_EQ(fetch_request.correlation_id, correlation_id);
    ASSERT_EQ(fetch_request.offset, offset);
    ASSERT_EQ(fetch_request.max_bytes, max_bytes);
}

TEST(TcpProtocolTests, tcp_response_to_bytes) {
    TcpResponse response_write{
        .correlation_id = {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1,
                            0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
        .response_code = 1,
        .payload = {}};
    auto bytes = response_write.to_bytes();
    ASSERT_EQ(bytes.size(),
              TCP_RESPONSE_HEADER_LEN + 4); // header len + 4 bytes for length
    auto response_read = TcpResponse::from_bytes(bytes);
    EXPECT_EQ(response_write.correlation_id, response_read.correlation_id);
    EXPECT_EQ(response_write.response_code, response_read.response_code);
    EXPECT_EQ(response_write.payload, response_read.payload);
    EXPECT_EQ(response_read.to_bytes(), bytes);
}

TEST(TcpProtocolTests, tcp_response_with_payload_to_bytes) {
    TcpResponse response_write{
        .correlation_id = {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1,
                            0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
        .response_code = 0,
        .payload = {{1, 2, 3, 4}}};
    auto bytes = response_write.to_bytes();
    ASSERT_EQ(bytes.size(), TCP_RESPONSE_HEADER_LEN + 8); // header len + 4 bytes for length
    auto response_read = TcpResponse::from_bytes(bytes);
    EXPECT_EQ(response_write.correlation_id, response_read.correlation_id);
    EXPECT_EQ(response_write.response_code, response_read.response_code);
    EXPECT_EQ(response_write.payload, response_read.payload);
    EXPECT_EQ(response_read.to_bytes(), bytes);
}
} // namespace broker
} // namespace kafka_lite
