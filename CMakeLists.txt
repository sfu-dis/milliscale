cmake_minimum_required(VERSION 3.15)

# No in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")

project(milliscale)

# For TPC-E egen to build: -Wno-address-of-packed-member
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -std=c++17 -Wno-address-of-packed-member")

set(LINK_FLAGS "-lnuma -lpthread -pthread -lglog -lgflags -luring")
link_libraries(${LINK_FLAGS})

# Store shared libraries under /lib
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Use masstree for index
add_definitions(-DMASSTREE)

# Disable prefetch in masstree
#add_definitions(-DNOPREFETCH)

# Assume 64-byte cache line
add_definitions(-DCACHELINE_SIZE=64)

# Page size for O_DIRECT
add_definitions(-DPAGE_SIZE=2048)

# 256B B-tree page size
add_definitions(-DBTREE_PAGE_SIZE=256)

add_definitions(-DGLOG_USE_GLOG_EXPORT)

find_package(AWSSDK REQUIRED CONFIG COMPONENTS s3)
find_package(Boost REQUIRED CONFIG COMPONENTS context)

set(MASSTREE_CONFIG "--enable-max-key-len=1024 --with-malloc=malloc")

if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include ${CMAKE_CURRENT_SOURCE_DIR}/index/masstree/config-debug.h")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
elseif(${CMAKE_BUILD_TYPE} STREQUAL "RelWithDebInfo")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include ${CMAKE_CURRENT_SOURCE_DIR}/index/masstree/config-release.h")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -g -shared-libgcc")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include ${CMAKE_CURRENT_SOURCE_DIR}/index/masstree/config-release.h")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
endif()

include_directories(dbcore)
include_directories(index)
include_directories(third-party/sparsehash/src)
include_directories(util)

add_subdirectory(benchmarks)
add_subdirectory(benchmarks/ycsb)
add_subdirectory(benchmarks/tpcc)
add_subdirectory(dbcore)
add_subdirectory(index)
add_subdirectory(util)

set_property(GLOBAL APPEND PROPERTY ALL_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/engine.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/tuple.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/txn.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/txn-ssn.cc
)

get_property(LIB_SRC GLOBAL PROPERTY ALL_SRC)

# Thread pool source
set_property(GLOBAL APPEND PROPERTY THREAD_POOL_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/dbcore/sm-thread.cpp
)
get_property(THREAD_POOL_SRC GLOBAL PROPERTY THREAD_POOL_SRC)

# Thread pool library
add_library(thread_pool SHARED ${THREAD_POOL_SRC})

# SI
add_library(milliscale_si SHARED ${LIB_SRC})
set_target_properties(milliscale_si PROPERTIES COMPILE_FLAGS "")

# Thread pool library with SSN enabled
#add_library(thread_pool_ssn SHARED ${THREAD_POOL_SRC})
#set_target_properties(thread_pool_ssn PROPERTIES COMPILE_FLAGS "-DSSN -DEARLY_SSN_CHECK")

# SI+SSN
#add_library(milliscale_si_ssn SHARED ${LIB_SRC})
#set_target_properties(milliscale_si_ssn PROPERTIES COMPILE_FLAGS "-DSSN -DEARLY_SSN_CHECK")

#add_executable(milliscale_SI_SSN ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/dbtest.cc)
#set_target_properties(milliscale_SI_SSN PROPERTIES COMPILE_FLAGS "-DSSN -DEARLY_SSN_CHECK")
#target_link_libraries(milliscale_SI_SSN milliscale_si_ssn thread_pool_ssn)

enable_testing()

if(BUILD_TESTS)
    set(BUILD_GMOCK OFF CACHE BOOL "Gmock Not Used")
    set(INSTALL_GTEST OFF CACHE BOOL "Static Link Gtest")

    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "No Testing For GoogleBenchmark Itself")
    set(BENCHMARK_ENABLE_EXECEPTIONS OFF CACHE BOOL "No Exception For GoogleBenchmark")
    set(BENCHMARK_ENABLE_LTO OFF CACHE BOOL "FIXME: Link Time Optimization leads to linking error")
    set(BENCHMARK_USE_LIBCXX ON CACHE BOOL "Build GoogleBenchmark With libc++")
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "Static Link GoogleBenchmark")

    add_subdirectory(tests)
endif()
