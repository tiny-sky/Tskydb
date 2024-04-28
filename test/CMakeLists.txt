cmake_minimum_required(VERSION 3.14)

# include(gtest)
include(GoogleTest)
link_directories(${CMAKE_ARCHIVE_OUTPUT_DIRECTORY})

file(GLOB_RECURSE TSKYDB_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*test.cc")

# "make all-tests"
add_custom_target(show-tests COMMAND ${CMAKE_CTEST_COMMAND} --show-only)
add_custom_target(all-tests COMMAND ${CMAKE_CTEST_COMMAND} --verbose)

# "make XXX_test"
foreach(Tskydb_test_source ${TSKYDB_TEST_SOURCES})
    get_filename_component(Tskydb_test_filename ${Tskydb_test_source} NAME)
    string(REPLACE ".cc" "" Tskydb_test_name ${Tskydb_test_filename})
    message(STATUS "${Tskydb_test_name}.")

    add_executable(${Tskydb_test_name} EXCLUDE_FROM_ALL ${Tskydb_test_source})
    add_dependencies(show-tests ${Tskydb_test_name})
    add_dependencies(all-tests ${Tskydb_test_name})

    gtest_discover_tests(${Tskydb_test_name}
            DISCOVERY_MODE PRE_TEST
            EXTRA_ARGS
            --gtest_catch_exceptions=0
            DISCOVERY_TIMEOUT 120
            PROPERTIES
            TIMEOUT 120
            )

    target_link_libraries(${Tskydb_test_name} Tskydb gtest gtest_main pthread)

    # Set test target properties
    set_target_properties(${Tskydb_test_name}
            PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
            COMMAND ${Tskydb_test_name}
            )

endforeach()

