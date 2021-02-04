#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

// swap helper function 
void swap(int * sub, int a, int b)
{
    int t = sub[a];
    sub[a] = sub[b];
    sub[b] = t;
}

// Main quicksort method adapted to use each rank's sub array and respective high and low points
/*
    1. Recursive function that picks a pivot somewhere in the middle of the sub array
    2. Swap the pivot and first element (some algorithms don't do this but it worked with this one)
    3. Partition the sub array starting at it's respective "low" element
        A. If the current element is less than the pivot element
        a. increment partitioning index to keep track of where to swap
        b. swap the current index and the partitioning index
        c. when loop breaks, swap the low and partitioning index to insure the pivot is back in the correct place
    4. Recurse quicksort on lower half of the array
    5. Recurse on array after step 4 to sort the higher half of the sub array


*/
void quicksort(int * sub, int low, int high)
{

    // base case?
    if (high <= 1)
        return;
    // find pivot and swap with first element (pivot has to be in the center of the sub array)
    int x = low+high/2;
    int pivot = sub[x];
    swap(sub, low, x);
    // partition sub array starting at it's respective low element
    // pi = partitioning index
    int pi = low;
    for (int i = low+1; i < low+high; i++)
        if (sub[i] < pivot) {
            pi++;
            swap(sub, i, pi);

        }
    // swap pivot correctly
    swap(sub, low, pi);
    // recurse below partition
    quicksort(sub, low, pi-low);
    // recure above partition
    quicksort(sub, pi+1, low+high-pi-1);
}


// Simple merge method to bring together two arrays
int * merge(int * v1, int n1, int * v2, int n2)
{
    int * result = (int *)malloc((n1 + n2) * sizeof(int));
    int i = 0;
    int j = 0;
    int k;
    for (k = 0; k < n1 + n2; k++) {
        if (i >= n1) {
            result[k] = v2[j];
            j++;
        }
        else if (j >= n2) {
            result[k] = v1[i];
            i++;
        }
        else if (v1[i] < v2[j]) {
            result[k] = v1[i];
            i++;
        }
        else {
            result[k] = v2[j];
            j++;
        }
    }
    return result;
}

int main(int argc, char ** argv)
{
    int n, size, subSize, tempSize, numranks, rank, i, step;
    int * data = NULL;
    int * sub;
    int * temp;
    MPI_Status status;
    if (argc < 2) {
        printf("Please give the number of elements you would like to sort and try again.\n");
        MPI_Finalize();
    }
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numranks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
     double startTime = MPI_Wtime();
    if (rank == 0) {
        n = atoi(argv[1]);
        if (n%numranks != 0) {
            size = n/numranks+1;
        }
        else {
            size = n/numranks;
        }
        data = (int *)malloc(numranks *size * sizeof(int));
        // Randomly assign values to array
        for (int i = 0; i < n; i++) {
            data[i] = rand() % n;
        }
    }
    // need a blocking call here to avoid segfault
    MPI_Barrier(MPI_COMM_WORLD);
    // start timer
   
    // broadcast size
    MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);
    // compute sub size
    if(n%numranks!=0) {
        size =  n/numranks+1;
    }
    else {
        size = n/numranks;
    }
    // scatter data
    sub = (int *)malloc(size * sizeof(int));
    MPI_Scatter(data, size, MPI_INT, sub, size, MPI_INT, 0, MPI_COMM_WORLD);
    // compute size of own sub array
    if(n >= size * (rank+1)) {
        subSize = size;
    }
    else {
        subSize =  n - size * rank;
    }
    // sort sub array
    quicksort(sub, 0, subSize);
    // "Step" method to merge together the "lower-half" rank's arrays and the "upper-half" rank's arrays
    // Works by merging each step up to log (numranks)
    step = 1;
    while (step<numranks)
    {
        if (rank%(2*step)==0)
        {
            if (rank+step<numranks)
            {
                MPI_Recv(&tempSize, 1, MPI_INT, rank+step, 0, MPI_COMM_WORLD, &status);
                temp = (int *)malloc(tempSize*sizeof(int));
                MPI_Recv(temp, tempSize, MPI_INT, rank+step, 0, MPI_COMM_WORLD, &status);
                sub = merge(sub, subSize, temp, tempSize);
                subSize = subSize+tempSize;
            }
        }
        else
        {
            int near = rank-step;
            MPI_Send(&subSize, 1, MPI_INT, near, 0, MPI_COMM_WORLD);
            MPI_Send(sub, subSize, MPI_INT, near, 0, MPI_COMM_WORLD);
            break;
        }
        step = step*2;
    }
    // stop the timer
    double endTime = MPI_Wtime();
    if (rank == 0) {

        // uncomment to check if sorting works
        //for (i = 0; i < subSize; i++)
            //printf("%d ", sub[i]);
        printf("%f,", endTime-startTime);
    }

    MPI_Finalize();
    return 0;
}