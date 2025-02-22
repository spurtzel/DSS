# Evaluation Plan Enumerator

## Overview

Enumerate all possible evaluation plans for a given sequence of event types. The program does not consider placement (i.e., it considers only multi-sink placements).

## Requirements

To compile the program, you need:

- A C++ compiler that supports C++20 (e.g., `g++` version 10 or later)
- A Unix-like environment (Linux/macOS) or Windows with MinGW

## Compilation

To compile the program, run the following command:

```sh
g++ enumerate_plans.cpp -Wall -pedantic -Werror -std=c++20 -O3 -o enumerate_plans
```

## Usage

The compiled executable `enumerate_plans` can be executed with the following syntax:

```sh
./enumerate_plans "<sequence>" "<pruning>" "<kleene>" "<negation>"
```

### Parameters:

- `<sequence>`: A string representing the sequence of event types (e.g., `"ABCD"`).
- `<pruning>`: A string of event types that are not sent through the network.
- `<kleene>`: A string of event types that represent Kleene operators (i.e., cannot be cut event types).
- `<negation>`: A string of event types that represent negation operators (i.e., cannot be cut event types).

### Example Execution:

```sh
./enumerate_plans "ABCD" "" "" ""
```

This example enumerates evaluation plans for the sequence `ABCD` with no pruning, no Kleene operators, and no negation operators.
