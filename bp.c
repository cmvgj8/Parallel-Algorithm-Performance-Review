#include <stdio.h> 
#include <stdlib.h>
#include "mpi.h"

void swap(int *xp, int *yp) 
{ 
    int temp = *xp; 
    *xp = *yp; 
    *yp = temp; 
} 
  
// A function to implement bubble sort 
void bubbleSort(int arr[], int split) 
{ 
   int i, j; 
   for (i = 0; i < split-1; i++)       
       // Last i elements are already in place    
       for (j = 0; j < split-i-1; j++)  
           if (arr[j] > arr[j+1]) 
              swap(&arr[j], &arr[j+1]); 
} 
  
/* Function to print an array */
void printArray(int arr[], int size) 
{ 
    int i; 
    for (i=0; i < size; i++) 
        printf("%d ", arr[i]); 
    printf("\n"); 
}

void main (int argc, char **argv){

	int numranks, rank, n, size;
	int *data = NULL;
	int *sub;
	// initialize MPI
	MPI_Init(&argc,&argv);
	MPI_Comm_size(MPI_COMM_WORLD,&numranks);
	MPI_Comm_rank(MPI_COMM_WORLD,&rank);
	if(rank == 0){
		if(argc < 2){
     			printf("Please give the number of elements you would like to sort and try again.\n");
		MPI_Finalize();
    	}
	n = atoi(argv[1]);
		if(n%numranks != 0){
			size = n/numranks+1;
		}
		else{
			size = n/numranks;
		}
		data = (int *)malloc(numranks * size * sizeof(int));
		//Randomly assign values to array
		for(int i = 0; i < n; i++){
			data[i] = rand() %n;
		}
	}
	MPI_Barrier(MPI_COMM_WORLD);
	double startTime = MPI_Wtime();
	MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);
	
 	if(n%numranks !=0){
		size = n/numranks+1;
	}	   	
	else{
		size = n/numranks;
	}
	//scatter data
	sub = (int *)malloc(size * sizeof(int));
	MPI_Scatter(data, size, MPI_INT, sub, size, MPI_INT, 0, MPI_COMM_WORLD);
	
	bubbleSort(sub,size);
	//printf("Rank: %d, sub: \n",rank);
	//printArray(sub,size); 
	
	MPI_Gather(sub, size, MPI_INT, data, size, MPI_INT, 0, MPI_COMM_WORLD);
	if(rank == 0){
		bubbleSort(data, size*numranks);
		double endTime = MPI_Wtime();
		printf("Numranks: %d, N: %d, Time: %f\n",numranks, n, endTime-startTime);
		
	}
   	free(data);
   	MPI_Finalize();  
   }