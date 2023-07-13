# CMake generated Testfile for 
# Source directory: /home/lys/ramboDB
# Build directory: /home/lys/ramboDB/cmake-build-debug
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(leveldb_tests "/home/lys/ramboDB/cmake-build-debug/leveldb_tests")
set_tests_properties(leveldb_tests PROPERTIES  _BACKTRACE_TRIPLES "/home/lys/ramboDB/CMakeLists.txt;392;add_test;/home/lys/ramboDB/CMakeLists.txt;0;")
add_test(c_test "/home/lys/ramboDB/cmake-build-debug/c_test")
set_tests_properties(c_test PROPERTIES  _BACKTRACE_TRIPLES "/home/lys/ramboDB/CMakeLists.txt;418;add_test;/home/lys/ramboDB/CMakeLists.txt;421;leveldb_test;/home/lys/ramboDB/CMakeLists.txt;0;")
add_test(rambo_test "/home/lys/ramboDB/cmake-build-debug/rambo_test")
set_tests_properties(rambo_test PROPERTIES  _BACKTRACE_TRIPLES "/home/lys/ramboDB/CMakeLists.txt;418;add_test;/home/lys/ramboDB/CMakeLists.txt;422;leveldb_test;/home/lys/ramboDB/CMakeLists.txt;0;")
add_test(env_posix_test "/home/lys/ramboDB/cmake-build-debug/env_posix_test")
set_tests_properties(env_posix_test PROPERTIES  _BACKTRACE_TRIPLES "/home/lys/ramboDB/CMakeLists.txt;418;add_test;/home/lys/ramboDB/CMakeLists.txt;430;leveldb_test;/home/lys/ramboDB/CMakeLists.txt;0;")
subdirs("third_party/googletest")
subdirs("third_party/benchmark")
