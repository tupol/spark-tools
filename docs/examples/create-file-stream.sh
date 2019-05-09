#!/usr/bin/env bash

if [[ $# -lt 2  ||  $# -gt 3 ]]; then
  echo $0 source_file output_folder [sleep_seconds]
  echo Take each line from the source_file and write it as an individual file in the output_folder every sleep_seconds
  exit -1
fi

file=$1

output_folder=$2

sleep_seconds=1
if [[ $# -eq 3 ]]; then
  sleep_seconds=$3
fi

lines=`cat $file`

mkdir -p $output_folder

index=0
while read -r line; do
  sleep $sleep_seconds
  index=$(($index+1))
  out=file_$index.part
  echo $line > $output_folder/$out
done < $file
