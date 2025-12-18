#!/bin/bash
# Cleanup script to kill lingering processes

echo "Cleaning up old processes..."
lsof -ti:5000,5001,5002,5003,5004,5050,5101,5102,5103,5104,8080 2>/dev/null | xargs kill -9 2>/dev/null || true

echo "Waiting for ports to release..."
sleep 2

echo "Cleanup complete!"
