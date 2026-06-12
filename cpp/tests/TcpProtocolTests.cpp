#include "../include/TcpProtocol.h"
#include <gtest/gtest.h>

namespace kafka_lite {
namespace broker {

class TcpProtocolTests : public ::testing::Test {
};

/*
1. parse length
2. parse magic bytes -> drop request if not correct
3. parse headers
    1. What can go wrong here, i.e. what needs to be validated?
        1. Wrong version
        2. Unknown type
        3. Unknown flags
4. read payload length
5. parse tcp request -> Instantiate TcpRequest and call TcpRequest::to_specialized_type()
    1. This does not need validation, client is responsible for ensuring payload integrity
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
}
}
