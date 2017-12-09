cmake_minimum_required(VERSION 3.8)
project(lightdb)

set(CMAKE_C_STANDARD 11)

set(SOURCE_FILES main.c node.h row.h page.h btree.h table.h values.h)
add_executable(lightdb ${SOURCE_FILES})