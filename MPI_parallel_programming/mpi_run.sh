#!/bin/bash
#Author:刘忠凯
#Date:2018/8/6
#Brief:
# 运行mpi

mpirun mkdir log
if [[ $? -ne 0 ]]; then
    echo "ERROR: 'mpirun mkdir log' failed!"
    exit -1
fi
mpirun mkdir output
if [[ $? -ne 0 ]]; then
    echo "ERROR: 'mpirun mkdir output' failedi!"
    exit -1
fi
mpirun ./mpi 100
if [[ $? -ne 0 ]]; then
    echo "ERROR: 'mpirun ./mpi' failed!"
    exit -1
fi