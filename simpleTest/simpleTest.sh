#!/bin/bash

input_file=$(pwd)/simpleTestCase.txt
executable="DolphinDBAPI_test"

cd ../build


if [ ! -f "$input_file" ]; then
  echo "$input_file is not found"
  exit 1
fi

for line in $(cat "$input_file"); do
  filter="$filter:$line"
done
echo "Running test cases with filter: $filter"

"./$executable" "--gtest_filter=$filter"
