cmake_minimum_required (VERSION 2.8)

project (my_consumer)

aux_source_directory(. DIR_SRCS)

add_executable(my_consumer ${DIR_SRCS})

TARGET_LINK_LIBRARIES(my_consumer rdkafka z pthread rt)
