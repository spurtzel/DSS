# DIPSUM Plan Generator and Sensitivity Analysis Experiments

## Overview
This repository contains our DIPSUM plan generator and sensitivity analysis experiments. The key directories are:

- **plan_enumerator/**: C++ code for exhaustively enumerating all multi-sink only DIPSUM plans.
- **results/**: Contains the results of executing sensitivity analysis experiments, saved as `.csv` files.

## Files
- **enumerate_plans**: Compiled executable of the C++ plan enumerator.
- **event_sourced_network.py**: Implementation of an event network.
- **plan_generator_sensitivity_experiment.py**: Implementation of various plan generators (C&C MuSi, BF MuSi, Min-Algorithm, and BF DIPSUM), our cost model, and corresponding sensitivity experiments.

## Running Sensitivity Analysis Experiments
To execute one of the sensitivity analysis experiments, open `plan_generator_sensitivity_experiment.py`, navigate to the `main()` function, and uncomment (`remove the #`) the corresponding experiment you wish to run. Then execute:

```sh
python plan_generator_sensitivity_experiment.py
```
