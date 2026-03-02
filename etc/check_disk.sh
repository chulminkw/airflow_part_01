#!/bin/bash
set -e # 오류 발생 시 즉각 중지
set -o pipefail # pipeline 오류 감시
MOUNT_DIR="$1"
THRESHOLD="$2"

echo "Checking disk usage for mount: $MOUNT_DIR"
echo "Threshold: ${THRESHOLD}%"

# 해당 mount volume이 존재하는지 확인. 문제 발생 시 exit 1
if [ ! -d "$MOUNT_DIR" ]; then
  echo "ERROR: Mount directory does not exist: $MOUNT_DIR"
  exit 1
fi

# POSIX-safe df output
# set -e 를 해도 pipeline의 맨마지막 명령어가 오류 반환하지 않으면 즉각 중지되지 않음. 
USAGE=$(df -P "$MOUNT_DIR" | awk 'NR==2 {gsub("%","",$5); print $5}')

echo "Current usage: ${USAGE}%"

# 해당 mount volume의 usage가 threshold가 지정값 이상이면 exit 1
if [ "$USAGE" -ge "$THRESHOLD" ]; then
  echo "ERROR: Disk usage exceeds threshold (${THRESHOLD}%)"
  exit 1
fi

echo "Disk usage is within safe limits"