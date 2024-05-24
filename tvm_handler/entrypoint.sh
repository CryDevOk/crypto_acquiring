#!/bin/bash

export APP_API_CPU_CORES=1 # $(( $(nproc) * 2 + 1 ))
rm -f /app/logs/*
python3 ./before_start.py

if [ $? -eq 1 ]; then
    echo "Startup script exited with code 1. Stopping container."
    exit 1
else
  echo "Startup script exited with code 0. Starting services."
  exec /usr/bin/python3 ./tx_handler.py &
  exec gunicorn restapi:app --log-level info --workers $APP_API_CPU_CORES --bind 0.0.0.0:"$PROC_HANDLER_PORT" --timeout 60 --worker-class uvicorn.workers.UvicornWorker --access-logfile -
fi
