# Pinned rapidfuzz-cpp release. To upgrade, change "version" in vcpkg.json and update SHA512 to the hash of
# https://github.com/rapidfuzz/rapidfuzz-cpp/archive/v<version>.tar.gz
vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO rapidfuzz/rapidfuzz-cpp
    REF "v${VERSION}"
    SHA512 f20e3bc23cc51c89df5ac26f3ab43daed5a3715eda1c47db59c6460e6a97f7a6afe4aa1f6557192d3b7b6a4591631192fa61d77a991dbe3ea38b68b0391e544f
    HEAD_REF main
)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
)

vcpkg_cmake_install()
vcpkg_cmake_config_fixup(
    PACKAGE_NAME rapidfuzz
    CONFIG_PATH lib/cmake/rapidfuzz
)

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/lib/")
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
