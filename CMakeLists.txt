cmake_minimum_required(VERSION 3.0)
project(dmon)

add_definitions(-Wall -Werror)

include_directories(
    rapidjson/include
    )

add_executable(dmon dmon.cpp)
