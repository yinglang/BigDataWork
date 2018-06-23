#!/usr/bin/env bash
# rm log/[0-9]*.txt

for pid in `ps -aux | grep graphlite | awk '{print $2}'`
do
    kill $pid 2> /dev/null
done

