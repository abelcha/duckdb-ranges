cmake_minimum_required(VERSION 2.8.12...3.29)

# Set extension name here
set(TARGET_NAME ranges)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

include_directories(src/include)

set(EXTENSION_SOURCES src/ranges_extension.cpp)

# Only define targets if this is a DuckDB build (when macros are available)
if(COMMAND build_static_extension)
  build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
  build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

  install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
endif()
