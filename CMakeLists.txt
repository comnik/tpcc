cmake_minimum_required(VERSION 3.0.2)
project(tpcc)

# Set CMake modules path
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

find_package(Boost REQUIRED COMPONENTS system)
find_package(TellDB REQUIRED)
find_package(Crossbow REQUIRED)
find_package(Jemalloc REQUIRED)

set(COMMON_SRC
    common/Protocol.cpp
    common/Util.cpp)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -mcx16")

find_package(Threads)

add_library(tpcc_common STATIC ${COMMON_SRC})
target_include_directories(tpcc_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(tpcc_common PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories(tpcc_common PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(tpcc_common PUBLIC ${Boost_LIBRARIES})
target_link_libraries(tpcc_common PUBLIC crossbow_logger)

set(SERVER_SRC
    server/main.cpp
    server/Connection.cpp
    server/Populate.cpp
    server/CreateSchema.cpp
    server/NewOrder.cpp
    server/Payment.cpp
    server/OrderStatus.cpp
    server/Delivery.cpp
    server/StockLevel.cpp)

set(CLIENT_SRC
    client/main.cpp
    client/Client.cpp)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/server/ch-tables/nation.tbl ${CMAKE_CURRENT_BINARY_DIR}/ch-tables/nation.tbl COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/server/ch-tables/region.tbl ${CMAKE_CURRENT_BINARY_DIR}/ch-tables/region.tbl COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/server/ch-tables/supplier.tbl ${CMAKE_CURRENT_BINARY_DIR}/ch-tables/supplier.tbl COPYONLY)

add_executable(tpcc_server ${SERVER_SRC})
target_link_libraries(tpcc_server PRIVATE tpcc_common telldb)
target_link_libraries(tpcc_server PUBLIC crossbow_allocator)

# Link against Jemalloc
target_include_directories(tpcc_server PRIVATE ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(tpcc_server PRIVATE ${Jemalloc_LIBRARIES})

add_executable(tpcc_client ${CLIENT_SRC})
target_link_libraries(tpcc_client PRIVATE tpcc_common ${CMAKE_THREAD_LIBS_INIT})

# Link against Jemalloc
target_include_directories(tpcc_client PRIVATE ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(tpcc_client PRIVATE ${Jemalloc_LIBRARIES})

set(USE_KUDU OFF CACHE BOOL "Build TPC-C for Kudu")
if(${USE_KUDU})
    set(kuduClient_DIR "/mnt/local/tell/kudu_install/share/kuduClient/cmake")
    find_package(kuduClient REQUIRED)
    find_package(Threads)
    message(STATUS "Kudu header found at ${KUDU_CLIENT_INCLUDE_DIR}")

    set(KUDU_SERVER_SRC 
        server/kudu.cpp
        server/CreateSchemaKudu.cpp
        server/PopulateKudu.cpp
        server/TransactionsKudu.cpp)

    add_executable(tpcc_kudu ${KUDU_SERVER_SRC} ${KUDU_INTERFACE_SRC})
    target_include_directories(tpcc_kudu PRIVATE ${KUDU_CLIENT_INCLUDE_DIR})
    target_compile_definitions(tpcc_kudu PRIVATE TPCC_KUDU)
    target_link_libraries(tpcc_kudu kudu_client)
    target_link_libraries(tpcc_kudu tpcc_common)
    target_link_libraries(tpcc_kudu ${CMAKE_THREAD_LIBS_INIT})
endif()
