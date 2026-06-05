#ifndef TCP_REQUEST_HH
#define TCP_REQUEST_HH

#include <boost/uuid.hpp>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace kafka_lite {
namespace broker {

static uint32_t TCP_RESPONSE_HEADER_LEN = 17;
static uint32_t TCP_REQUEST_HEADER_LEN = 20; // Without optional headers

enum class RequestType {
    Append,
    Fetch,
    //Heartbeat,
    //ReplicaSync,
};

struct AppendRequest {
    std::vector<uint8_t> payload;
};

struct FetchRequest {
    uint64_t offset;
    size_t max_bytes;
};

struct TcpHeaders {
    boost::uuids::uuid correlation_id;
    uint8_t protocol_version;
    RequestType type;
    uint16_t flags;
    bool from_bytes(const std::vector<uint8_t> &bytes);
    std::vector<uint8_t> to_bytes();
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
};


} // namespace broker
} // namespace kafka_lite
#endif
