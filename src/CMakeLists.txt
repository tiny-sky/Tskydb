add_subdirectory(lrucache)
add_subdirectory(util)
add_subdirectory(skiplist)

add_library(Tskydb STATIC ${ALL_OBJECT_FILES})

set(TSKYDB_LIBS
        Tskydb_lrucache
        Tskydb_skiplist
        Tskydb_util
        )

find_package(Threads REQUIRED)

target_link_libraries(
        Tskydb
        ${TSKYDB_LIBS})
