#ifndef BROKERCLIENT_HH
#define BROKERCLIENT_HH

#include "TcpProtocol.h"
#include <cstdint>
#include <vector>

namespace kafka_lite {
namespace broker {

class BrokerClient {
  public:
    BrokerClient(unsigned int port);
    TcpResponse append(const std::vector<uint8_t> &payload);
    TcpResponse fetch(uint64_t offset, uint32_t max_bytes);

  private:
    unsigned int port_;
};

}

}

#endif
