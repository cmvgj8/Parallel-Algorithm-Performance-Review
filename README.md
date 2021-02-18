# Parallel-Algorithm-Performance-Review

Description: 
This repository contains the final report as well as the parallel code for the parallel algorithm performance review. The parallel programs are written in C and used the MPI library to gain access to parallel functionality. The three algorithms in question are bubble sort, merge sort, and quick sort. To make graphs based off the time it took for each of the nodes to compte their portion of the work, paraprof was used.

Run:
1. The parallel programs were compiled using the mpicc compiler
2. The executable was submitted to Wentworth's super computer along with the number of nodes desired for that run
3. Output and profile files were created in the current working directory. The output files contained the timings for that run. The profile files were taken out of the super computer onto a pc and sent into the paraprof program. From there, an image of the work distribution was created.
