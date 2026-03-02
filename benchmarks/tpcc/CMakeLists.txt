set_property(GLOBAL APPEND PROPERTY ALL_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/tpcc-config.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../bench.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../dbtest.cc
)

set(BUILD_SHARED_LIBS ON CACHE STRING "Link to shared libraries by default.")

add_executable(tpcc_SI_sequential ${CMAKE_CURRENT_SOURCE_DIR}/tpcc-sequential.cc)
set_target_properties(tpcc_SI_sequential PROPERTIES COMPILE_FLAGS "")
target_link_libraries(tpcc_SI_sequential milliscale_si thread_pool ${AWSSDK_LINK_LIBRARIES} boost_context)
