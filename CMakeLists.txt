cmake_minimum_required(VERSION 3.0.2)
project(tpcc)

find_package(Boost REQUIRED COMPONENTS system)
find_package(TellDB REQUIRED)
find_package(Crossbow REQUIRED)
find_package(LLVM REQUIRED)

set(COMMON_SRC
    common/Protocol.cpp)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -mcx16 -ftemplate-depth=512")

add_library(tpcc_common STATIC ${COMMON_SRC})
# Workaround for link failure with GCC 5 (GCC Bug 65913)
#if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
#    target_link_libraries(tpcc_common PUBLIC atomic)
#endif()
target_include_directories(tpcc_common PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories(tpcc_common PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(tpcc_common PUBLIC ${Boost_LIBRARIES})
target_link_libraries(tpcc_common PUBLIC telldb)

set(SERVER_SRC
    server/main.cpp
    server/Connection.cpp
    server/Populate.cpp
    server/CreateSchema.cpp)

set(CLIENT_SRC
    client/main.cpp
    client/Client.cpp)

add_executable(tpcc_server ${SERVER_SRC})
target_include_directories(tpcc_server PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(tpcc_server tpcc_common)
add_executable(tpcc_client ${CLIENT_SRC})
target_include_directories(tpcc_client PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(tpcc_client tpcc_common)
