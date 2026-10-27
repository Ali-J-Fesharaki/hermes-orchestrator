#!/usr/bin/env bash
# start_supervisor.sh: Starts the 10-minute Platform Supervisor Agent
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${DIR}/scripts/supervisor.sh" start "$@"
