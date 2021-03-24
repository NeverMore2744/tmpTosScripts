#!/bin/bash

source common.sh

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
    bin="bin/${bin_suffix}"
    output_dir="processed/$output_suffix/"
    analyzed_dir="result/${trace_prefix[$K]}${analyzed_suffix}/"
    echo "bin = $bin, analyzed_dir = $analyzed_dir, output_dir = $output_dir"
    if [[ ! -d $output_dir ]]; then
      mkdir -p $output_dir
    fi
    if [[ ! -d bin ]]; then
      mkdir bin
    fi

    echo "Processing ${display_names[$K]} on $disp ... output at directory $output_dir"

    g++ $src -o $bin -std=c++11 
    if [[ $? -ne 0 ]]; then 
      echo "Compile failed"
      exit
    fi

    output=tmp.txt
    $bin $analyzed_dir ${trace_file_paths[$K]} ${params[@]} > $output

    cat $output
#
    cat $output | while read line; do
      mv $line $output_dir/${trace_prefix[$K]}_$line
    done
    rm -f tmp.txt
  done
}

#process_file "pc_hot_groups" "_hot_groups" "hot_groups" "src/process_hot_lbas_groups.cc" "Hot LBAs"
process_file "ar" "_ar" "ar" "src/process_ar.cc" "RAR and WAR"
process_file "aw" "_aw" "aw" "src/process_aw.cc" "RAW and WAW"
