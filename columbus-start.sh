#!/bin/bash
args=("$@")
if [ ${#args[@]} -lt 3 ]; then
    echo "Usage: qualified-hosts-filename master-ip-address master-port-number [virtual-env-path]"
else
    while IFS='' read -r line || [[ -n "$line" ]]; do
        echo "Starting worker on $line"
        if [ ${#args[@]} -ge 4 ]; then
            ssh ${line} "source $4/bin/activate; nohup python -m colorker $2 $3 > /dev/null 2>&1"&
        else
            ssh ${line} "nohup python -m colorker $2 $3 > /dev/null 2>&1"&
        fi
    done < "$1"
fi
