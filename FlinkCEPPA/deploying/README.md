# Running the FlinkCEP Pattern Aggregation Experiment

## Overview
This directory contains all scripts for generating config and trace files, as well as example inputs for running FlinkCEP Pattern Aggregation (PA) and FlinkCEP centralized. The key directories are:

- **generate_configs/**: Scripts and tools for generating config files.
- **generate_traces/**: Scripts and tools for generating trace files.
- **example_inputs/**: Contains the Flink PA and centralized inputs from our experiments.

## Running an Example
To execute an example, use the following command:

```sh
./run_local example_inputs/pattern_aggregation_2500 5
```

After executing the script you should see the following output:
```
starting node 0
starting node 1
starting node 2
starting node 3
starting node 4
```

... and after a minute, it starts reading the traces:
```
starting inputs on 0
starting inputs on 1
starting inputs on 2
starting inputs on 3
starting inputs on 4
```

### Parameters:
- `example_inputs/pattern_aggregation_2500`: Path to the example directory.
- `5`: Number of nodes.

The execution does not terminate by itself, thus, check in the corresponding example_inputs directory, if the execution finished (i.e., there was nothing written into the log files anymore)

## Log Analysis
Each example input directory contains a `log_analyzer.py` script for analyzing generated logs.

To execute the log analyzer, cd into the directory and run:

```sh
python log_analyzer.py 5
```

### Parameters:
- `5`: Number of created logs (equal to the specified number of nodes).

### Output:
The results are written to `result.txt`, which contains:
- The duration of execution.
- The resulting aggregate.
- A comparison to the ground truth aggregate.
