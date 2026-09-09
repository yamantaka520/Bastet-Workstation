#!/bin/sh
# Test-only provider fixture. Never forwards work to a real provider.
while [ "$#" -gt 0 ]; do
    case "$1" in
        --model) mode=$2; shift 2 ;;
        *) shift ;;
    esac
done
IFS= read -r request
case "$mode" in
    delayed-init)
        IFS= read -r trigger
        printf '%s\n' '{"event":"init","conversation_id":"poll-test","init":{}}'
        exec sleep 5
        ;;
    activity)
        printf '%s\n' '{"event":"init","conversation_id":"poll-test","init":{}}'
        IFS= read -r trigger
        sleep 0.2
        printf '%s\n' '{"event":"step_update","step_update":{"conversation_id":"poll-test","step_type":"agent_response","text_delta":"# Answer"}}'
        sleep 0.2
        printf '%s\n' '{"event":"result","result":{"conversation_id":"poll-test","status":"SUCCESS"}}'
        ;;
    silent) exec sleep 5 ;;
    cancel)
        printf '%s\n' '{"event":"init","conversation_id":"cancel-test","init":{}}'
        exec sleep 5
        ;;
    *) exit 2 ;;
esac
