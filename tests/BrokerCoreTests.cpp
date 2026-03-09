#include "../include/BrokerCore.h"
#include <gtest/gtest.h>

namespace kafka_lite {
namespace broker {

class BrokerCoreTests : public ::testing::Test {
  private:
    std::filesystem::path dir_;

  public:
    std::filesystem::path getDir() { return dir_; }

  protected:
    void SetUp() override {
        dir_ = std::filesystem::current_path() / "Core";
    }
    void TearDown() override { std::filesystem::remove_all(dir_); }
};

/*
1. Multithreaded appends (with and without rollover)
    1. n threads appending
    2. record results (i.e. offsets) in n vectors -> todo: write offset into the record itself
    3. ensure that offsets have increased monotonically
    4. Compare contents?
2. Multithreaded reads (with and without rollover): 
    1. same as appending, but now start reading during writes
    2. Assert monotonous offsets, maybe compare contents for the returned records
3. Any way to test stopping?
    1. Right now no stop function is implemented, so need to do that first
    2. Need to make sure that a stop leads to complete reads or writes, so especially for reads I need to be careful.
       For this I first need to know that crash recovery indeed takes place (is called), since I need to restart to read again.
4. Maybe also a small test for starting that crash recovery has taken place. 
   This does not have to be exhaustive, i.e. just do some single threaded writes, ensure these are correct.
   Then restart core and ensure contents are still there, i.e. no torn writes or anything.
*/


}
}
