# Distributed State Summary

## Overview
This directory contains the implementation and example execution of our prototype for DIPSUM and centralized DIPSUM.

- **example/**: Contains an example execution of our prototype for both DIPSUM and centralized DIPSUM.
- **partial_state_summary/**: Implementation of the Partial State Summary.
- **sender.py**: Handles TCP network communication, processes input aggregate streams, and creates output aggregate streams for the Distributed State Summary.

## Requirements
To run the prototype, you need:

- Python 3.10.9 or later

## Usage
### Running the Example Execution
Navigate to the `example/DIPSUM` directory and execute the provided script to execute the prototype:

```sh
cd example/DIPSUM
./run_nodes
```

After the experiment is done (i.e., all python scripts terminated), run:

```py
python addup_aggregates_and_messages.py
```
to examine the resulting aggregate value as well as the execution duration.
