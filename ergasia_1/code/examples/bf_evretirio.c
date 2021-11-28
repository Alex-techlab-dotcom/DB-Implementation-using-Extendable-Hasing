#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/bf.h"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }



int main() {
    int fd1;
    int requestedBlock=1;
    char *dataOfSecondBlock;
    char retriveAddress[20];
    char *data;
    BF_Block *block, *secondBlock, *blockPointer, *block3;
    BF_Block_Init(&block);// it allocates the suitable space in memory
    BF_Block_Init(&secondBlock);
    BF_Block_Init(&block3);

    
    CALL_OR_DIE(BF_Init(LRU));//it uses the LRU method to clear the unneeded blocks from the buffer
    // CALL_OR_DIE(BF_CreateFile("data1.db"))
    CALL_OR_DIE(BF_OpenFile("data1.db", &fd1));// it gives the data1.db file a specific ID(lets say ID=11)

    /* block1 */
    CALL_OR_DIE(BF_AllocateBlock(fd1, block));
    data = BF_Block_GetData(block);// we take info from the block we just allocated to data1.db
    memset(data, 11, BF_BUFFER_SIZE);// we change the data
    printf("BLOCK1 CONTAINS %d\n", data[0]);
    printf("THE ADDRESS OF BLOCK ONE IS : %p\n", (void *) block);
    BF_Block_SetDirty(block);
    //CALL_OR_DIE(BF_UnpinBlock(block));

    /* block3 */
    CALL_OR_DIE(BF_AllocateBlock(fd1, block3));
    data = BF_Block_GetData(block3);// we take info from the block we just allocated to data1.db
    memset(data, 33, BF_BUFFER_SIZE);// we change the data
    printf("BLOCK3 CONTAINS %d\n", data[0]);
    printf("THE ADDRESS OF BLOCK3 IS : %p\n", (void *) block3);
    BF_Block_SetDirty(block3);

    /* block2 evretirio */
    CALL_OR_DIE(BF_AllocateBlock(fd1, secondBlock));
    char blockAddressToString[20];
    //WE COPY THE ADDRESS OF BLOCK1 TO A STRING
    sprintf(blockAddressToString, "%p", (void *) block);


    printf("TEST0\n");
    dataOfSecondBlock = BF_Block_GetData(secondBlock);// we take info from the block we just allocated to data1.db
    // we change the data
    printf("TEST1\n");
    printf("THE SIZE OF THE ADDRESS  OF BLOCK1 IS : %lu\n", strlen(blockAddressToString));
    /* we store the address of block1 to the data to secondBlock*/
    memmove(dataOfSecondBlock, blockAddressToString, strlen(blockAddressToString));

    /* we store the address of block3 to the data to secondBlock*/
    sprintf(blockAddressToString, "%p", (void *) block3);
    printf("THE SIZE OF THE ADDRESS OF BLOCK3 IS : %lu\n", strlen(blockAddressToString));
    printf("THE ADDRESS OF BLOCK3 IS : %s\n", blockAddressToString);
    memmove(dataOfSecondBlock + 14, blockAddressToString, strlen(blockAddressToString));


    /* we retrive the address of block1*/
    requestedBlock=1;
    memmove(retriveAddress, dataOfSecondBlock+(requestedBlock-1)* strlen(blockAddressToString), strlen(blockAddressToString));
    printf("ADDRESS IS %s\n", retriveAddress);
    long fromStringToHex = strtol(retriveAddress, NULL, 16);//we convert the string to hex number
    blockPointer = (BF_Block *) fromStringToHex;
    printf("THE ADDRESS OF BLOCK ONE(blockPointer) IS : %p\n", (void *) blockPointer);
    if ( block == blockPointer )printf("EQUALS\n");
    data= BF_Block_GetData(blockPointer);
    printf("BLOCK1 CONTAINS %d\n",data[0]);


    requestedBlock=2;
    memmove(retriveAddress, dataOfSecondBlock+(requestedBlock-1)* strlen(retriveAddress), strlen(retriveAddress));
    printf("RETRIEVED ADDRESS OF BLOCK3 IS %s\n", retriveAddress);
    if ( strcmp(retriveAddress, blockAddressToString) == 0 )printf("BINGO\n");

    fromStringToHex = strtol(retriveAddress, NULL, 16);
    blockPointer = (BF_Block *) fromStringToHex;
    data= BF_Block_GetData(blockPointer);
    printf("BLOCK3 CONTAINS %d\n",data[0]);


    BF_Block_SetDirty(secondBlock);
    CALL_OR_DIE(BF_UnpinBlock(block));
    CALL_OR_DIE(BF_UnpinBlock(block3));
    CALL_OR_DIE(BF_UnpinBlock(secondBlock));




    CALL_OR_DIE(BF_CloseFile(fd1));// we close the specific buffer
    CALL_OR_DIE(BF_Close());// we close the BLOCK_LEVEL and we write all the blocks from the buffer back to the disk;


}


