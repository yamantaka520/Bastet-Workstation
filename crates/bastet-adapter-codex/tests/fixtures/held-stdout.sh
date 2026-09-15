#!/bin/sh
# Synthetic fixture only; run through /bin/sh with an explicit lifetime mode.
mode=$1
sleep 30 &
printf '%s\n' '{"method":"fixture/ready","params":{}}'
if [ "$mode" = live ]; then
    cat >/dev/null
fi
