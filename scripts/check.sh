#!/bin/bash

source common.sh

flag=0

check() {
  dir_path=$1
  files_path=$2
  cat $files_path | while read line; do
    filename=${dir_path}/${line}.csv
    if [[ ! -f $filename ]]; then
      echo "not exist: $filename ... exiting"
      return 1 
    fi
  done
}

echo "Check whether trace files exist..."
echo "AliCloud ...." 
check $ALICLOUD_PATH $ALICLOUD_FILE_PATH
if [[ $? -eq 1 ]]; then
  exit
fi

echo "TencentCloud ...." 
check $TENCENTCLOUD_PATH $TENCENTCLOUD_FILE_PATH
if [[ $? -eq 1 ]]; then
  exit
fi

echo "MSRC ...." 
check $MSRC_PATH $MSRC_FILE_PATH
if [[ $? -eq 1 ]]; then
  exit
fi

