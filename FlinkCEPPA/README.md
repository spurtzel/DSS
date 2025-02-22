# FlinkCEP Pattern Aggregation Code and Experiment

## Overview
This directory contains our FlinkCEP Pattern Aggregation (PA) implementation. The key directories are:
    
- **deploying/**: Scripts and tools for running the FlinkCEP PA experiment.
- **java-cep/**: FlinkCEP PA code.
- **python/**: TCP network communication.

## Compilation
The FlinkCEP PA code can be compiled using **Maven** (which has to be installed first). To compile, navigate to the `java-cep` directory and run:

```sh
cd java-cep
mvn clean package
```

