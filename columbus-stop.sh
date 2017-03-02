#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "Stopping worker on $line"
    ssh ${line} "pkill python > /dev/null 2>&1"&
done < "$1"
