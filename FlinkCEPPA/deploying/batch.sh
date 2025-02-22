#!/bin/bash
for D in example_inputs/multiquery*; do
 echo "$D"
 ./run_local "$D" 5 15
 rm -rf /tmp/flink-*
done         

