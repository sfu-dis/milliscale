## Contribution Guidelines

We conform to [Google C++ Style Guidelines](https://google.github.io/styleguide/cppguide.html). 
In particular:
- For C/C++, we use `.c`/`.cc` and `.h` for source files and header files, respectively.
- Each line of code should not exceed 100 characters.
- Source files are organised by the functionality they implement, so a directory includes both the header and source files;
- All Noname code must be wrapped in the `noname` namespace;
- Each component further has its own namespace, under `noname`;
- Each source file should start with the copyright and licence notice.
- No importing or aliasing (`using`) of namespace at all.
- Use `CHECK_EQ/NE` instead of `CHECK(a == b)/(a != b)`.
- When it comes to integer types, prefer `uint64_t`, `uint32_t`, `uint16_t`, `uint8_t`.
- For indentation, each tab is strictly 2 characters and should be "expanded" to become spaces.
- `CamelCase` for class, struct, function and method names.
- `snake_case` for local variables and member variables.
- `kConstant` or `UPPER_SNAKE_CASE` for constants.
- Logging/output should be done using glog instead of `std::cerr` or `std::cout`.
- For debug-time only logs/output, use `DLOG(...)`; `LOG(...)` otherwise.
- Test cases should be written using GTest.
- Command-line parameters should use GFlags.
- Knobs/parameters for each component should be defined in `src/config/config.ini`. The build system will auto-generate the corresponding `config.h`. See more details under the "Parameters" section below.
- No extra space, e.g., for casting use `reinterpret_cast<uint32_t>(...)` rather than `reinterpret_cast<uint32_t> (...)`.
- Use `struct` rather than `class`, for easier testing.

You may use "clang-format --style=Google" to make sure your code conforms to them. 
