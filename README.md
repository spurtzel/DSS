# DIPSUM: Distributed Pattern Summaries for Efficient CEP Aggregates

## Overview
This repository contains the implementation of the DIPSUM framework, including the plan generator, cost model, sensitivity experiments, and FlinkCEP Pattern Aggregation (PA). The key directories are:

- **plan_generator/**: Contains the DIPSUM plan generator, cost model, and sensitivity experiments.
- **prototype/**: Prototypical implementation of the DIPSUM framework, including the partial StateSummary, TCP network communication, and experiment examples.
- **FlinkCEPPA/**: FlinkCEP Pattern Aggregation (PA) implementation, including experiment examples.

## System Requirements
All experiments were executed on an Arch Linux distribution. 

### Python Dependencies
- Python3 (version >= 3.10.9)
- Ensure the following Python libraries are installed before running the experiments:

```sh
pip install numpy argparse json logging numpy pathlib statistics
```

The project requires the following Python modules:
- `numpy` (numerical computations)
- `argparse` (argument parsing)
- `json` (handling JSON data)
- `logging` (logging utilities)
- `pathlib` (file system operations)
- `statistics` (statistical computations)

### Linux/Unix Programs
The execution scripts utilize the following Linux/Unix programs:
- `echo`, `wait`, `head`, `trap`, `cat`, `flock`

### Build Tools
- Make
- CMake (version >= 3.12.0)
- C++ compiler supporting C++20 or higher


