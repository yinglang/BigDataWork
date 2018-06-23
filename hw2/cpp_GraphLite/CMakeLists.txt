cmake_minimum_required(VERSION 3.8)
project(cpp_GraphLite)
set(CMAKE_CXX_STANDARD 11)

# visited https://blog.csdn.net/woshinia/article/details/11060797
add_definitions("-std=c++0x -g -O3 -fPIC")
set(HADOOP_DIR "/home/hui/ide/hadoop/hadoop-2.6.5")
set(GRAPHLITE_DIR "/home/hui/github/GraphLite/GraphLite-0.20")
include_directories(GRAPH ${HADOOP_DIR}/include ${GRAPHLITE_DIR}/include)
# PageRank
add_library(PageRankVertex SHARED PageRankVertex.cc)

# SSSP
add_library(SSSP SHARED SSSP.cc)

# KCore
add_library(KCore SHARED KCore.cpp)

# GraphColor
add_library(GraphColor SHARED GraphColor.cc)

# DirectedTriangleCount
add_library(DirectedTriangleCount SHARED DirectedTriangleCount.cc)

add_executable(main main.cpp)