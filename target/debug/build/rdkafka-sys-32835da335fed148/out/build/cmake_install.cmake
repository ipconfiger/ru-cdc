# Install script for directory: /Users/alex/.cargo/registry/src/rsproxy.cn-8f6827c7555bfaf8/rdkafka-sys-4.7.0+2.3.0/librdkafka

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/Users/alex/Projects/workspace/ru-cdc/target/debug/build/rdkafka-sys-32835da335fed148/out")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Debug")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

# Set default install directory permissions.
if(NOT DEFINED CMAKE_OBJDUMP)
  set(CMAKE_OBJDUMP "/usr/bin/objdump")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/RdKafka" TYPE FILE FILES
    "/Users/alex/Projects/workspace/ru-cdc/target/debug/build/rdkafka-sys-32835da335fed148/out/build/generated/RdKafkaConfig.cmake"
    "/Users/alex/Projects/workspace/ru-cdc/target/debug/build/rdkafka-sys-32835da335fed148/out/build/generated/RdKafkaConfigVersion.cmake"
    "/Users/alex/.cargo/registry/src/rsproxy.cn-8f6827c7555bfaf8/rdkafka-sys-4.7.0+2.3.0/librdkafka/packaging/cmake/Modules/FindLZ4.cmake"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/RdKafka/RdKafkaTargets.cmake")
    file(DIFFERENT _cmake_export_file_changed FILES
         "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/RdKafka/RdKafkaTargets.cmake"
         "/Users/alex/Projects/workspace/ru-cdc/target/debug/build/rdkafka-sys-32835da335fed148/out/build/CMakeFiles/Export/a1c6bd80150ccef2e736c8ff7566f1db/RdKafkaTargets.cmake")
    if(_cmake_export_file_changed)
      file(GLOB _cmake_old_config_files "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/RdKafka/RdKafkaTargets-*.cmake")
      if(_cmake_old_config_files)
        string(REPLACE ";" ", " _cmake_old_config_files_text "${_cmake_old_config_files}")
        message(STATUS "Old export file \"$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/RdKafka/RdKafkaTargets.cmake\" will be replaced.  Removing files [${_cmake_old_config_files_text}].")
        unset(_cmake_old_config_files_text)
        file(REMOVE ${_cmake_old_config_files})
      endif()
      unset(_cmake_old_config_files)
    endif()
    unset(_cmake_export_file_changed)
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/RdKafka" TYPE FILE FILES "/Users/alex/Projects/workspace/ru-cdc/target/debug/build/rdkafka-sys-32835da335fed148/out/build/CMakeFiles/Export/a1c6bd80150ccef2e736c8ff7566f1db/RdKafkaTargets.cmake")
  if(CMAKE_INSTALL_CONFIG_NAME MATCHES "^([Dd][Ee][Bb][Uu][Gg])$")
    file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/RdKafka" TYPE FILE FILES "/Users/alex/Projects/workspace/ru-cdc/target/debug/build/rdkafka-sys-32835da335fed148/out/build/CMakeFiles/Export/a1c6bd80150ccef2e736c8ff7566f1db/RdKafkaTargets-debug.cmake")
  endif()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/share/licenses/librdkafka" TYPE FILE FILES "/Users/alex/.cargo/registry/src/rsproxy.cn-8f6827c7555bfaf8/rdkafka-sys-4.7.0+2.3.0/librdkafka/LICENSES.txt")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/Users/alex/Projects/workspace/ru-cdc/target/debug/build/rdkafka-sys-32835da335fed148/out/build/src/cmake_install.cmake")
  include("/Users/alex/Projects/workspace/ru-cdc/target/debug/build/rdkafka-sys-32835da335fed148/out/build/src-cpp/cmake_install.cmake")

endif()

if(CMAKE_INSTALL_COMPONENT)
  set(CMAKE_INSTALL_MANIFEST "install_manifest_${CMAKE_INSTALL_COMPONENT}.txt")
else()
  set(CMAKE_INSTALL_MANIFEST "install_manifest.txt")
endif()

string(REPLACE ";" "\n" CMAKE_INSTALL_MANIFEST_CONTENT
       "${CMAKE_INSTALL_MANIFEST_FILES}")
file(WRITE "/Users/alex/Projects/workspace/ru-cdc/target/debug/build/rdkafka-sys-32835da335fed148/out/build/${CMAKE_INSTALL_MANIFEST}"
     "${CMAKE_INSTALL_MANIFEST_CONTENT}")
