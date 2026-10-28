cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME rapidfuzz)

# rapidfuzz-cpp is installed by vcpkg from the version pinned in vcpkg_ports/rapidfuzz-cpp
find_package(rapidfuzz CONFIG REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/rapidfuzz_extension.cpp
    src/rapidfuzz_characters.cpp
    src/rapidfuzz_fuzz_functions.cpp
    src/rapidfuzz_distance_functions.cpp
    src/rapidfuzz_alignment_functions.cpp
    src/rapidfuzz_extract_functions.cpp
    src/query_farm_telemetry.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} rapidfuzz::rapidfuzz)
target_link_libraries(${LOADABLE_EXTENSION_NAME} rapidfuzz::rapidfuzz)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
