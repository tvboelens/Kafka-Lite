cmake_minimum_required(VERSION 3.16)
# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_C_COMPILER /opt/homebrew/bin/gcc-15)
set(CMAKE_CXX_COMPILER /opt/homebrew/bin/g++-15)

project(Kafka-Lite LANGUAGES C CXX)

# include necessary directories
#include_directories(/opt/homebrew/include)

include(FetchContent)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)

FetchContent_MakeAvailable(googletest)


# Add source files
set(SOURCES
    src/main.cpp
    src/Log.cpp
    src/Segment.cpp
    src/AppendQueue.cpp
)

set(TEST_SOURCES
    tests/TestSuite.cpp
    )


# Define the executable
add_executable(Broker ${SOURCES})

enable_testing()
add_executable(TestSuite ${TEST_SOURCES})
target_link_libraries(TestSuite 
                                GTest::gtest_main
                                GTest::gtest)
include(GoogleTest)
gtest_discover_tests(TestSuite)

