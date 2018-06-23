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

# SimRank
add_library(SimRank SHARED SimRank.cc)

# Naive SimRank
add_library(NaiveSimRank SHARED NaiveSimRank.cc)

add_executable(main main.cpp CSimRank.h)