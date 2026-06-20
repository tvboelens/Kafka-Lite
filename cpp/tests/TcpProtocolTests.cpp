#include "../include/ByteSwap.h"
#include "../include/TcpProtocol.h"
#include <cstdint>
#include <cstring>
#include <gtest/gtest.h>
#include <stdexcept>
#include <system_error>
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

struct TcpRequestToFetchRequestTest {
    TcpHeaders headers;
    uint64_t offset;
    uint32_t max_bytes;
};

TEST(TcpProtocolTests, tcp_request_to_fetch_request) {
    std::vector<TcpRequestToFetchRequestTest> tests = {
        {{{{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
            0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
          0,
          RequestType::Fetch,
          0},
         200,
         1024},
        {{{{0x6b, 0xa7, 0xb8, 0x00, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
            0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
          4,
          RequestType::Fetch,
          3},
         200,
         1024}};
    for (const auto &test : tests) {
        auto payload = TcpRequest::make_payload(test.offset, test.max_bytes);
        TcpRequest request{.headers = test.headers, .payload = payload};
        auto alternative = request.to_specialized_type();
        ASSERT_TRUE(std::holds_alternative<FetchRequest>(alternative));
        auto fetch_request = std::get<FetchRequest>(alternative);
        EXPECT_EQ(fetch_request.correlation_id, test.headers.correlation_id);
        EXPECT_EQ(fetch_request.offset, test.offset);
        EXPECT_EQ(fetch_request.max_bytes, test.max_bytes);
    }
}

TEST(TcpProtocolTests, tcp_response_to_bytes) {
    std::vector<TcpResponse> responses{
        {.correlation_id = {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1,
                             0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
         .response_code = 1,
         .payload = {}},
        {.correlation_id = {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1,
                             0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
         .response_code = 0,
         .payload = {{1, 2, 3, 4}}},
        {.correlation_id = {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1,
                             0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
         .response_code = 12,
         .payload = {{1, 2, 3, 4}}},
    };
    for (const auto &response : responses) {
        auto bytes = response.to_bytes();
        ASSERT_EQ(bytes.size(), TCP_RESPONSE_HEADER_LEN + 4 +
                                    response.payload.value_or({}).size());
        auto response_read = TcpResponse::from_bytes(bytes);
        EXPECT_EQ(response.correlation_id, response_read.correlation_id);
        EXPECT_EQ(response.response_code, response_read.response_code);
        EXPECT_EQ(response.payload, response_read.payload);
        EXPECT_EQ(response_read.to_bytes(), bytes);
    }
}

struct TcpRequestFromOffsetEcTest {
    boost::uuids::uuid correlation_id;
    uint64_t offset;
    std::error_code ec;
    uint8_t expected_rc;
};

TEST(TcpProtocolTests, tcp_response_from_offset_ec) {
    std::vector<TcpRequestFromOffsetEcTest> tests = {
        {
            {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
              0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
            1024,
            {},
            0,
        },
        {{{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
           0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
         0,
         std::make_error_code(std::errc::not_connected),
         0x80},
        {{{0x6b, 0xa7, 0xb8, 0x10, 0x9f, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
           0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
         10,
         std::make_error_code(std::errc::io_error),
         0x81},
        {{{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
           0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
         0,
         std::make_error_code(std::errc::not_connected),
         0x80},
        {{{0x6b, 0xa7, 0xb8, 0x10, 0x9f, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
           0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
         10,
         std::make_error_code(std::errc::address_in_use),
         0xff},
        {{{0x6b, 0xa7, 0xb8, 0x10, 0x9f, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
           0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
         10,
         std::make_error_code(std::errc::bad_message),
         0x05}};
    for (const auto &test : tests) {
        auto response = TcpResponse::makeResponse(test.correlation_id,
                                                  test.offset, test.ec);
        EXPECT_EQ(response.correlation_id, test.correlation_id);
        EXPECT_EQ(response.response_code, test.expected_rc);
        bool has_ec = test.ec ? true : false;
        ASSERT_NE(response.payload.has_value(), has_ec);
        if (!test.ec) {
            uint64_t offset = 0;
            ASSERT_EQ(response.payload->size(), sizeof(offset));
            std::memcpy(&offset, response.payload->data(), sizeof(offset));
            if (!byteswap::is_big_endian())
                offset = byteswap::byteswap64(offset);
            EXPECT_EQ(offset, test.offset);
        }
    }
}

struct TcpRequestFromParseErrorTest {
    boost::uuids::uuid correlation_id;
    ParseError error;
    uint8_t expected_rc;
};

TEST(TcpProtocolTests, tcp_response_from_parse_error) {
    std::vector<TcpRequestFromParseErrorTest> tests = {
        {
            {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
              0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
            ParseError::ERR_MISSING_CORRELATION_ID,
            0x01,
        },
        {{{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
           0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
         ParseError::ERR_UNKNOWN_TYPE,
         0x03},
        {{{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
           0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
         ParseError::NO_ERROR,
         0x00},
        {{{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
           0xc0, 0x5f, 0xd4, 0x30, 0xc8}},
         ParseError::ERR_UNSUPPORTED_FLAGS,
         0x04},
        {{{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
           0xc0, 0x5f, 0xd4, 0x30, 0xc8}},
         ParseError::ERR_UNSUPPORTED_VERSION,
         0x02}};
    for (const auto &test : tests) {
        if (test.error == ParseError::NO_ERROR) {
            EXPECT_THROW(
                TcpResponse::makeErrorResponse(test.correlation_id, test.error),
                std::logic_error);
        } else {
            auto response =
                TcpResponse::makeErrorResponse(test.correlation_id, test.error);
            if (test.error == ParseError::ERR_MISSING_CORRELATION_ID) {
                boost::uuids::uuid correlation_id = {
                    {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                     0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};
                EXPECT_EQ(response.correlation_id, correlation_id);
            } else {
                EXPECT_EQ(response.correlation_id, test.correlation_id);
            }
            EXPECT_EQ(response.response_code, test.expected_rc);
            ASSERT_FALSE(response.payload.has_value());
        }
    }
}

struct TcpRequestFromFetchResultEcTest {
    boost::uuids::uuid correlation_id;
    FetchResult fetch_result;
    std::error_code ec;
    uint8_t expected_rc;
};

TEST(TcpProtocolTests, tcp_response_from_fetch_result_ec) {
    std::vector<TcpRequestFromFetchResultEcTest> tests = {
        {
            {{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
              0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
            {{1, 2, 3, 4}, {}},
            {},
            0,
        },
        {
            {{0x66, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
              0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
            {{1, 2, 3, 4}, {}},
            std::make_error_code(std::errc::not_connected),
            0x80,
        },
        {
            {{0x66, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
              0xc0, 0x4f, 0xd4, 0x30, 0xc8}},
            {{5, 7, 9, 11}, {}},
            std::make_error_code(std::errc::io_error),
            0x81,
        },
        {
            {{0x66, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00,
              0xc0, 0x4f, 0xd4, 0x30, 0xf8}},
            {{2, 4, 6, 8}, {}},
            std::make_error_code(std::errc::executable_format_error),
            0xff,
        }};
    for (const auto &test : tests) {
        auto response = TcpResponse::makeResponse(test.correlation_id,
                                                  test.fetch_result, test.ec);
        EXPECT_EQ(response.correlation_id, test.correlation_id);
        EXPECT_EQ(response.response_code, test.expected_rc);
        bool has_ec = test.ec ? true : false;
        ASSERT_NE(response.payload.has_value(), has_ec);
        if (!test.ec) {
            ASSERT_TRUE(response.payload.has_value());
            EXPECT_EQ(response.payload, test.fetch_result.result_buf);
        } else {
            EXPECT_FALSE(response.payload.has_value());
        }
    }
}

} // namespace broker
} // namespace kafka_lite
