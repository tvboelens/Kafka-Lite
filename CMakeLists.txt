cmake_minimum_required(VERSION 3.16)
# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_C_COMPILER /opt/homebrew/bin/gcc-15)
set(CMAKE_CXX_COMPILER /opt/homebrew/bin/g++-15)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

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
set(BROKER_LIB_SOURCES
	src/Log.cpp
	src/Segment.cpp
	src/AppendQueue.cpp
)

set(TEST_SOURCES
    tests/TestSuite.cpp
    )


# Define the executable
add_executable(Broker src/main.cpp ${SOURCES})

add_executable(TestSuite ${TEST_SOURCES}
${BROKER_LIB_SOURCES})
target_link_libraries(TestSuite 
    PRIVATE
                                GTest::gtest_main
                                GTest::gtest)
enable_testing()
include(GoogleTest)
gtest_discover_tests(TestSuite)

