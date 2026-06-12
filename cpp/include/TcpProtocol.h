#ifndef TCP_REQUEST_HH
#define TCP_REQUEST_HH

#include "../include/Segment.h"
#include <boost/uuid.hpp>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace kafka_lite {
namespace broker {

using boost::uuids::uuid;

static uint32_t TCP_RESPONSE_HEADER_LEN = 17;
static uint32_t TCP_REQUEST_HEADER_LEN = 20; // Without optional headers
static uint8_t PROTOCOL_VERSION = 0;

enum class RequestType {
    Append,
    Fetch,
    // Heartbeat,
    // ReplicaSync,
};

enum class ParseError {
    NO_ERROR = 0x00,
    ERR_MISSING_CORRELATION_ID = 0x01,
    ERR_UNSUPPORTED_VERSION = 0x02,
    ERR_UNKNOWN_TYPE = 0x03,
    ERR_UNSUPPORTED_FLAGS = 0x04,
};

struct AppendRequest {
    boost::uuids::uuid correlation_id;
    std::vector<uint8_t> payload;
};

struct FetchRequest {
    boost::uuids::uuid correlation_id;
    uint64_t offset;
    size_t max_bytes;
};

struct TcpHeaders {
    TcpHeaders() = default;
    TcpHeaders(const uuid &correlation_id, uint8_t ptcl_version,
               RequestType type, uint16_t flags);
    uuid correlation_id;
    uint8_t protocol_version;
    RequestType type;
    uint16_t flags;
    bool from_bytes(const std::vector<uint8_t> &bytes);
    std::vector<uint8_t> to_bytes();

    ParseError getParseError() { return parse_error; }

  private:
    ParseError parse_error;
};

struct TcpRequest {
    TcpHeaders headers;
    std::vector<uint8_t> payload;
    std::variant<AppendRequest, FetchRequest> to_specialized_type();
};

struct TcpResponse {
    boost::uuids::uuid correlation_id;
    uint8_t response_code;
    std::optional<std::vector<uint8_t>> payload;
    std::vector<uint8_t> to_bytes();

    static TcpResponse
    createErrorResponse(const boost::uuids::uuid &correlation_id,
                        ParseError error);
    static TcpResponse makeResponse(const boost::uuids::uuid &correlation_id,
                                    uint64_t offset, const std::error_code &ec);
    static TcpResponse makeResponse(const boost::uuids::uuid &correlation_id,
                                    const FetchResult &result,
                                    const std::error_code &ec);
};

} // namespace broker
} // namespace kafka_lite
#endif
