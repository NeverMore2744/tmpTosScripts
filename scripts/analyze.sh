#!/bin/bash

source common.sh

analyze_one_line() {
  bin_suffix=$1;
  output_suffix=$2;
  src=$3;
  disp=$4;

  for ((K=0; K<${#define_names[@]}; K++)); do
    bin="bin/${trace_prefix[$K]}${bin_suffix}"
    output="result/${trace_prefix[$K]}${output_suffix}"
    property_file="result/${trace_prefix[$K]}_property"
    echo "Analyzing ${display_names[$K]} on $disp ... output at $output"

    g++ $src -o $bin -std=c++11 -D${define_names[$K]}
    if [[ $? -ne 0 ]]; then 
      echo "Compile failed"
      exit
    fi

# Check whether the analysis has been finished
    numVols=`wc -l < ${trace_file_paths[$K]}`
    numAnalyzed=`wc -l < $output`

    if [[ $numVols -eq $numAnalyzed ]]; then
      echo "Analyzed ${display_names[$K]}. skip to the next one"
      continue
    fi

    rm -f $output
    cat ${trace_file_paths[$K]} | while read line; do
      trace_file=${trace_paths[$K]}/$line.csv
      $bin $line $trace_file $property_file >> $output
    done
  done
}

analyze_multiple_files() {
  bin_suffix=$1;
  output_suffix=$2;
  src=$3;
  disp=$4;

  for ((K=0; K<${#define_names[@]}; K++)); do
    bin="bin/${trace_prefix[$K]}${bin_suffix}"
    output_dir="result/${trace_prefix[$K]}${output_suffix}"
    if [[ ! -d $output_dir ]]; then
      mkdir $output_dir
    fi

    property_file="result/${trace_prefix[$K]}_property"
    echo "Analyzing ${display_names[$K]} on $disp ... output at directory $output_dir"

    g++ $src -o $bin -std=c++11 -D${define_names[$K]}
    if [[ $? -ne 0 ]]; then 
      echo "Compile failed"
      exit
    fi

    cat ${trace_file_paths[$K]} | while read line; do
      trace_file=${trace_paths[$K]}/$line.csv
      output=${output_dir}/$line.data
      sz=`ls -s ${output} 2>/dev/null | awk '{print $1;}'`
      if [[ $? -ne 0 || $sz -eq 0 ]]; then  # Not exist, or empty file
        $bin $line $trace_file $property_file >> $output
      else
        echo "Volume $line in ${display_names[$K]} is analyzed before, skip"
      fi
    done
  done
}

echo "0. analyze the properties of three traces .. ";
analyze_one_line "_property" "_property" "src/analyze_property.cc" "properties"

echo "1. analyze basic statistics of three traces ..";
analyze_multiple_files "_bs" "_bs" "src/analyze_basic_stats.cc" "basic statistics"

echo "2. analyze read distances of three traces ..";
analyze_multiple_files "_bs" "_bs" "src/analyze_basic_stats.cc" "basic statistics"
