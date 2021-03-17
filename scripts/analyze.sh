#!/bin/bash

source common.sh

analyze_multiple_files() {
  bin_suffix=$1;
  output_suffix=$2;
  src=$3;
  disp=$4;
  params=()

  if [[ $# -gt 4 ]]; then
    params=("${[@:5]}");
    echo "$params[@]"
  fi

  for ((K=0; K<${#define_names[@]}; K++)); do
    bin="bin/${trace_prefix[$K]}${bin_suffix}"
    output_dir="result/${trace_prefix[$K]}${output_suffix}"
    if [[ ! -d $output_dir ]]; then
      mkdir $output_dir
    fi

    property_file="result/${trace_prefix[$K]}_property.data"
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
        $bin $line $trace_file $property_file ${params[@]} >> $output
      else
        echo "Volume $line in ${display_names[$K]} is analyzed before, skip"
      fi
    done
  done
}

merge() {
  input_prefix=$1
  input_suffix=$2;
  output_suffix=$3;
  
  for ((K=0; K<${#define_names[@]}; K++)); do
    output="result/${trace_prefix[$K]}${output_suffix}"
    rm -f $output
    
    cat ${trace_file_paths[$K]} | while read line; do
      input="result/${input_prefix}${trace_prefix[$K]}${input_suffix}${line}.data"
      if [[ ! -f $input ]]; then
        echo "Error: input $input not exist"
        return 1
      fi
      cat $input >> $output
    done
  done
}

echo "0. analyze the properties of three traces .. ";
analyze_multiple_files "_property" "_property" "src/analyze_property.cc" "properties"
merge "" "_property/" "_property.data" 
echo ""

echo "1. analyze basic statistics of three traces ..";
analyze_multiple_files "_bs" "_bs" "src/analyze_basic_stats.cc" "basic statistics"
echo ""

echo "2. analyze read distances (RAR and WAR) of three traces ..";
analyze_multiple_files "_ar" "_ar" "src/analyze_after_read_distance.cc" "RAR and WAR"
echo ""

echo "3. analyze write distances (RAW and WAW) of three traces ..";
analyze_multiple_files "_aw" "_aw" "src/analyze_after_write_distance.cc" "RAW and WAW" 
echo ""

echo "4. analyze update distances of three traces ..";
analyze_multiple_files "_ud" "_ud" "src/analyze_update_distance.cc" "update distances" 
echo ""

echo "5. analyze randomness of three traces ..";
analyze_multiple_files "_rand" "_rand" "src/analyze_randomness.cc" "randomness" 32 32 

