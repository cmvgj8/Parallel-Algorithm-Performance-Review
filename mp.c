/* C program for Merge Sort */
#include <stdio.h> 
#include <stdlib.h> 
#include "mpi.h"
  
// Merges two subarrays of arr[]. 
// First subarray is arr[l..m] 
// Second subarray is arr[m+1..r] 
void merge(int arr[], int l, int m, int r) 
{ 
    int i, j, k; 
    int n1 = m - l + 1; 
    int n2 = r - m; 
  
    /* create temp arrays */
    int L[n1], R[n2]; 
  
    /* Copy data to temp arrays L[] and R[] */
    for (i = 0; i < n1; i++) 
        L[i] = arr[l + i]; 
    for (j = 0; j < n2; j++) 
        R[j] = arr[m + 1 + j]; 
  
    /* Merge the temp arrays back into arr[l..r]*/
    i = 0; // Initial index of first subarray 
    j = 0; // Initial index of second subarray 
    k = l; // Initial index of merged subarray 
    while (i < n1 && j < n2) { 
        if (L[i] <= R[j]) {
            arr[k] = L[i]; 
            i++; 
        } 
        else { 
            arr[k] = R[j]; 
            j++; 
        } 
        k++; 
    } 

    /* Copy the remaining elements of L[], if there 
       are any */
    while (i < n1) { 
        arr[k] = L[i]; 
        i++; 
        k++; 
    } 
  
    /* Copy the remaining elements of R[], if there 
       are any */
    while (j < n2) { 
        arr[k] = R[j]; 
        j++; 
        k++; 
    }
    
} 
  
/* l is for left index and r is right index of the 
   sub-array of arr to be sorted */
void mergeSort(int arr[], int l, int r) 
{ 
    if (l < r) { 
        // Same as (l+r)/2, but avoids overflow for 
        // large l and h 
        int m = l + (r - l) / 2; 
  
        // Sort first and second halves 
        mergeSort(arr, l, m); 
        mergeSort(arr, m + 1, r); 
  
        merge(arr, l, m, r); 
    } 
} 
  
/* UTILITY FUNCTIONS */
/* Function to print an array */
void printArray(int A[], int size) 
{ 
    int i; 
    for (i = 0; i < size; i++) 
        printf("%d ", A[i]); 
    printf("\n"); 
} 
  
/* Driver program to test above functions */
int main(int argc, char **argv) 
{ 
    	int rank, numranks, size, subSize, n;
	int *data = NULL;
	int *sub;
	int *temp;
    	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numranks);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Status status;
	
	if(argc < 2){
		printf("Please give number of elements you would like to sort and try again");
		MPI_Finalize();
	}    
	if(rank == 0){
		n = atoi(argv[1]);
		if(n%numranks !=0){
			size = n/numranks+1;
		}else{
			size = n/numranks;
		}
		data = (int *)malloc(numranks * size * sizeof(int));
		//Randomly assign values to array
		for(int i = 0; i < n; i++){
			data[i] = rand() % n;
		}
		//printArray(data, numranks *size);
		//printf("\n");
	}
	//Check to see if i actually need this
	MPI_Barrier(MPI_COMM_WORLD);
	
	double startTime = MPI_Wtime();
	//Broadcast size
	MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);
	//Compute subsize
	if(n%numranks!=0){
		size = n/numranks+1;
	}
	else{
		size = n/numranks;
	}
	// scatter data
	sub = (int *)malloc(size * sizeof(int));
	MPI_Scatter(data, size, MPI_INT, sub, size, MPI_INT, 0, MPI_COMM_WORLD);
	//printf("Rank :%d , sub: \n",rank);
	//printArray(sub, size);
	//printf("\n");
	mergeSort(sub, 0, size);
	//printf("Rank After: %d, sub: \n",rank);
	//printArray(sub, size);
	MPI_Gather(sub, size, MPI_INT, data, size, MPI_INT,0, MPI_COMM_WORLD);
	if(rank == 0){
		//printf("After Gather: \n");
		//printArray(data, size * numranks);
		if(numranks != 1){
			//mergeSort(data, 0, size*numranks);
			merge(data, 0, n/2, n);
		}
		//printf("after mergeSort(2): \n");
		//printArray(data, size * numranks);
		double endTime = MPI_Wtime();
		printf("Numranks: %d, N: %d, Time: %f\n",numranks, n, (endTime-startTime));
	} 
	MPI_Finalize();
	return 0;
} 
