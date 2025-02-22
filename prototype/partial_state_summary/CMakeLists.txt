cmake_minimum_required(VERSION 3.12..3.28)
project(sequence_accumulator VERSION 0.0.1)

if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_options("$<$<C_COMPILER_ID:MSVC>:/utf-8>")
add_compile_options("$<$<CXX_COMPILER_ID:MSVC>:/utf-8>")

include(FetchContent)
set(FETCHCONTENT_QUIET FALSE)

set(BOOST_INCLUDE_LIBRARIES multiprecision)
set(BOOST_ENABLE_CMAKE ON)

FetchContent_Declare(
	Boost
	URL https://github.com/boostorg/boost/releases/download/boost-1.87.0/boost-1.87.0-cmake.7z
	USES_TERMINAL_DOWNLOAD TRUE 
	GIT_PROGRESS TRUE   
	DOWNLOAD_NO_EXTRACT FALSE
	SYSTEM ON
)
FetchContent_MakeAvailable(Boost)

FetchContent_Declare(
	fmt
	GIT_REPOSITORY https://github.com/fmtlib/fmt.git
	GIT_TAG 10.0.0
	GIT_SHALLOW TRUE
	SYSTEM ON
)
FetchContent_MakeAvailable(fmt)

set(CXXOPTS_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(CXXOPTS_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CXXOPTS_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(CXXOPTS_ENABLE_WARNINGS OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
	cxxopts
	GIT_REPOSITORY https://github.com/jarro2783/cxxopts.git
	GIT_TAG v3.1.1
	GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(cxxopts)

add_executable(sequence_accumulator

	src/edgelist.cpp src/edgelist.hpp
	src/event.hpp
	src/execution_state_counter.hpp src/execution_state_counter_impl.hpp
	src/nfa.cpp src/nfa.hpp
	src/regex.cpp src/regex.hpp
	src/sequence_accumulator.cpp
)

if(MSVC)
	target_compile_options(sequence_accumulator PRIVATE /W4)
else()
	target_compile_options(sequence_accumulator PRIVATE -Wall -pedantic -Werror)
endif()


target_link_libraries(sequence_accumulator PRIVATE fmt::fmt Boost::multiprecision cxxopts)
set_property(TARGET sequence_accumulator PROPERTY CXX_STANDARD 20)


add_executable(sequence_accumulator_fat

	src/edgelist.cpp src/edgelist.hpp
	src/event.hpp
	src/execution_state_counter.hpp src/execution_state_counter_impl.hpp
	src/nfa.cpp src/nfa.hpp
	src/regex.cpp src/regex.hpp
	src/sequence_accumulator.cpp
)

if(MSVC)
	target_compile_options(sequence_accumulator_fat PRIVATE /W4)
else()
	target_compile_options(sequence_accumulator_fat PRIVATE -Wall -pedantic -Werror)
endif()

set_target_properties(sequence_accumulator_fat PROPERTIES LINK_SEARCH_START_STATIC ON)
set_target_properties(sequence_accumulator_fat PROPERTIES LINK_SEARCH_END_STATIC ON)
target_link_options(sequence_accumulator_fat PRIVATE -static-libgcc -static-libstdc++ -static)

target_link_libraries(sequence_accumulator_fat PRIVATE fmt::fmt Boost::multiprecision cxxopts)
set_property(TARGET sequence_accumulator_fat PROPERTY CXX_STANDARD 20)
set_target_properties(sequence_accumulator_fat PROPERTIES EXCLUDE_FROM_ALL TRUE)
