cmake_minimum_required(VERSION 3.14)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_COMPILER "/usr/bin/clang++")

project(Tskydb
        VERSION 2024.4
        DESCRIPTION "KV database"
        LANGUAGES C CXX)

# Compilre setup
if(NOT DEFINED LOCK_SANITIZER)
        set(LOCK_SANITIZER address)
endif()

# Find GTest library
find_package(GTest REQUIRED)
if(NOT GTest_FOUND)
    include(gtest)
else()
    message(STATUS "GTest library found.")
endif()

message(STATUS "${LOCK_SANITIZER} sanitizer will be enabled.")
message(STATUS "CMAKE_CXX_COMPILER: ${CMAKE_CXX_COMPILER_ID}")

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fsanitize=${LOCK_SANITIZER} -fno-omit-frame-pointer -fno-optimize-sibling-calls")

message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

# Output directory.
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Includes.
set(TSKYDB_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src)

include_directories(${TSKYDB_SRC_INCLUDE_DIR})
include_directories(BEFORE src)

enable_testing()

# Other CMake modules
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")

add_subdirectory(src)
add_subdirectory(test)