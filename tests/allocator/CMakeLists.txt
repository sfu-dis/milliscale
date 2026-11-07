add_executable(test_allocators test_main.cpp)

add_test(NAME allocators COMMAND test_allocators)