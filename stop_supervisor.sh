#!/usr/bin/env bash
# stop_supervisor.sh: Stops the Platform Supervisor Agent
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${DIR}/scripts/supervisor.sh" stop "$@"
