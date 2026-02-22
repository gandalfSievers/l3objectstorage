#!/bin/sh
set -e

# Signal forwarding - ensure clean shutdown
trap 'kill -TERM $PID; wait $PID' TERM INT

# Execute the main binary with all arguments
exec /app/l3-object-storage "$@"
