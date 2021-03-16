#!/bin/bash

TENCENTCLOUD_PATH="/sdc_data/tencent_trace/"
TENCENTCLOUD_FILE_PATH="./etc/tc_traces"
ALICLOUD_PATH="/sdb_data/ali_trace/"
ALICLOUD_FILE_PATH="./etc/ali_traces"
MSRC_PATH="/home/jhli/MSR-Cambridge/"
MSRC_FILE_PATH="./etc/msr_traces"

trace_prefix=("ali" "tc" "msr");
define_names=("ALICLOUD" "TENCENTCLOUD" "MSRC");
display_names=("AliCloud" "TencentCloud" "MSRC");
trace_paths=($ALICLOUD_PATH $TENCENTCLOUD_PATH $MSRC_PATH);
trace_file_paths=($ALICLOUD_FILE_PATH $TENCENTCLOUD_FILE_PATH $MSRC_FILE_PATH);
