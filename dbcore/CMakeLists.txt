set(DBCORE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/dlog.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dynarray.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/pcommit.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/serial.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/size-encode.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/task.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/s3-client.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-alloc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-common.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-config.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-exceptions.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-table.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-object.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-oid-alloc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-oid.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/xid.cpp
)

set_property(GLOBAL APPEND PROPERTY ALL_SRC ${DBCORE_SRC})
