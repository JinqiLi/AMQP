cmake_minimum_required (VERSION 2.8)

project (my_producer)

aux_source_directory(. DIR_SRCS)

add_executable(my_producer ${DIR_SRCS})

TARGET_LINK_LIBRARIES(my_producer rdkafka z pthread rt)