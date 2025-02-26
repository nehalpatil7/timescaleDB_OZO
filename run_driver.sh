#!/bin/bash

# argument exist check
if [ $# -eq 0 ]; then
  echo "Usage: $0 <source_file.cpp>"
  exit 1
fi

SOURCE_FILE="$1"

# file exist check
if [ ! -f "$SOURCE_FILE" ]; then
  echo "Error: File '$SOURCE_FILE' not found!"
  exit 1
fi

# extract the binary name
BINARY_NAME="${SOURCE_FILE%.*}"

# compile with required libraries
g++ -std=c++17 "$SOURCE_FILE" -o "$BINARY_NAME" -lpqxx -lpq
if [ $? -ne 0 ]; then
  echo "Compilation failed!"
  exit 1
fi

echo "Driver file compiled!"

# run binary as background process to protect it from hangups
nohup ./"$BINARY_NAME" > output.log 2>&1 &

# PID of background process
echo "Driver is running in the background with PID $!"
