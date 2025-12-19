cmake_minimum_required(VERSION 3.16)
project(Kafka-Lite)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)

# include necessary directories
include_directories(/opt/homebrew/include)

# Find the required packages
find_package(GTest REQUIRED)


# Add source files
set(SOURCES
    src/main.cpp
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

