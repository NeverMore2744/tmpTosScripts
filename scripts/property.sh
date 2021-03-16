#!/bin/bash

source common.sh

for ((K=0; K<${#define_names[@]}; K++)); do
  bin="bin/${trace_prefix[$K]}_property"
  output="result/${trace_prefix[$K]}_property"
  echo "Analyzing ${display_names[$K]} ... output at $output"

  g++ src/analyze_property.cc -o $bin -std=c++11 -D${define_names[$K]}
  if [[ $? -ne 0 ]]; then 
    echo "Compile failed"
    exit
  fi

  rm -f $output
  cat ${trace_file_paths[$K]} | while read line; do
    trace_file=${trace_paths[$K]}/$line.csv
    $bin $line $trace_file >> $output
  done
done
