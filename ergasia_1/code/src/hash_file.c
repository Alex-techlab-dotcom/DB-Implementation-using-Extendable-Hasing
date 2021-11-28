#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include "../include/bf.h"
#include "../include/hash_file.h"
#include "../examples/exhashtable.h"

#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {      \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

HT_ErrorCode HT_Init() {
    //insert code here
    return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {

    int blockFileID;
   // CALL_BF(BF_Init(LRU));//it uses the LRU method to clear the unneeded blocks from the buffer
    CALL_BF(BF_CreateFile(filename))
    CALL_BF(BF_OpenFile(filename, &blockFileID));// it gives the data1.db file a specific ID(lets say ID=11)

    /* Block Initialization*/
    BF_Block *firstBlock, *secondBlock;
    BF_Block_Init(&firstBlock);// it allocates the suitable space in memory
    BF_Block_Init(&secondBlock);// it allocates the suitable space in memory

    /* we allocate the blocks at the end of the BF_Block file */
    CALL_BF(BF_AllocateBlock(blockFileID, firstBlock));
    CALL_BF(BF_AllocateBlock(blockFileID, secondBlock));

    char * firstBlockData = BF_Block_GetData(firstBlock);

    memmove(firstBlockData, "HF", strlen("HF"));
    memset(firstBlockData + strlen("HF")*sizeof(char), depth, sizeof(int));
    BF_Block_SetDirty(firstBlock);

    int sz=(int)pow(2,depth);
    HashTable* hashTable = malloc(sizeof(struct HashTable));
    hashTable->h_array=malloc(sz*sizeof(struct HashIndex));
    for(int i=0; i<sz; i++){
        BF_Block_Init(&hashTable->h_array[i].blockPointer); //initialize hash table blocks
    }

    char ht_addr[20];
    sprintf(ht_addr, "%p", (void *) hashTable);

    char * secondBlockData = BF_Block_GetData(secondBlock);
    memmove(secondBlockData,ht_addr,strlen(ht_addr));
    BF_Block_SetDirty(secondBlock);
    return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc) {
    //insert code here
    return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
    //insert code here
    return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
    //insert code here
    return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
    //insert code here
    return HT_OK;
}

