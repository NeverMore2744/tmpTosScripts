#!/bin/bash

outputfiles=()

process_file() {
  bin_suffix=$1;
  analyzed_suffix=$2
  output_suffix=$3;
  src=$4;
  disp=$5;
  params=()

  if [[ $# -gt 5 ]]; then
    params=("${@:6}");
    echo "$params"
  fi

  for ((K=0; K<${#define_names[@]}; K++)); do
    bin="bin/${trace_prefix[$K]}${bin_suffix}"
    output_dir="processed/$output_suffix/"
    analyzed_dir="result/${trace_prefix[$K]}${analyzed_suffix}/"
    if [[ ! -d $output_dir ]]; then
      mkdir $output_dir
    fi

    echo "Processing ${display_names[$K]} on $disp ... output at directory $output_dir"

    g++ $src -o $bin -std=c++11 
    if [[ $? -ne 0 ]]; then 
      echo "Compile failed"
      exit
    fi

    output_files=${output_dir}/$line.data
    output=tmp.txt
#    sz=`ls -s ${output} 2>/dev/null | awk '{print $1;}'`
#    if [[ $? -ne 0 || $sz -eq 0 ]]; then  # Not exist, or empty file
      $bin $analyzed_prefix $trace_file_paths[$K] ${params[@]} > $output
#    else
#      echo "Volume $line in ${display_names[$K]} is analyzed before, skip"
#    fi

    cat $output

    set -x
    cat $output | while read line; do
      mv $line $output_dir/${trace_prefix[$K]}_$line
    done
  done
}

process_file "_hot_lbas" "_hot_lbas" "hot_lbas" "src/process_hot_lbas.cc" "Hot LBAs"
