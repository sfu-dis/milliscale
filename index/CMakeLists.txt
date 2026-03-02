include_directories(masstree)
include_directories(btree)
include_directories(dash)

add_subdirectory(masstree)

set(DBCORE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/btree_wrapper.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/hash_wrapper.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/masstree_wrapper.cc
)

set_property(GLOBAL APPEND PROPERTY ALL_SRC ${DBCORE_SRC})
