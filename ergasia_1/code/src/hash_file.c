#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include "../include/bf.h"
#include "../include/hash_file.h"
#include "../examples/exhashtable.h"



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
    for ( int i = 0; i < MAX_OPEN_FILES; ++i ) {
        OpenHashFiles[i].FileName=NULL;
    }
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

    //char* blockData;
    char * secondBlockData = BF_Block_GetData(secondBlock);
    int sz=(int)pow(2,depth);

    for ( int i = 0; i <sz ; ++i ) {
        BF_Block *block;
        BF_Block_Init(&block);
        CALL_BF(BF_AllocateBlock(blockFileID, block));
        memset(secondBlockData+(2*i)*sizeof(int) ,i, sizeof(int));
        memset(secondBlockData+(2*i)*sizeof(int)+sizeof(int) ,i+3, sizeof(int));

    }

   /* int sz=(int)pow(2,depth);
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
    CALL_BF(BF_CloseFile(blockFileID));*/
    return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc) {
    //insert code here
    for ( int i = 0; i <MAX_OPEN_FILES ; ++i ) {
        if(OpenHashFiles[i].FileName==NULL){
            OpenHashFiles[i].FileName= malloc((strlen(fileName)+1)*sizeof(char));
            strcpy(OpenHashFiles[i].FileName,fileName);
            CALL_BF(BF_OpenFile(fileName,&OpenHashFiles[i].BFid));
            *indexDesc=i;
            return HT_OK;
        }
    }
    printf("NO SPACE AVAILABLE FOR HASHFILE :%s\n",fileName);
    return HT_ERROR;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
    //insert code here
    if(indexDesc<0 || indexDesc>MAX_OPEN_FILES){
        printf("INDEX OUT OF TABLE LENGTH\n");
        return HT_ERROR;
    }
    if(OpenHashFiles[indexDesc].FileName==NULL)printf("THERE IS NO OPEN FILE AT THIS INDEX\n");
    CALL_BF(BF_CloseFile(OpenHashFiles[indexDesc].BFid));
    free(OpenHashFiles[indexDesc].FileName);
    OpenHashFiles[indexDesc].FileName=NULL;
    return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
    //insert code here
    BF_Block *secondBlock;
    char * secondBlockData;
    char HtAddress[20];

    /* we retrieve the address of the hashtable from the second block*/
    CALL_BF(BF_GetBlock(OpenHashFiles[indexDesc].BFid,2,secondBlock));
    secondBlockData= BF_Block_GetData(secondBlock);
    /*the secondBlockData contains only the address of the hashtable so we take its lenght*/
    memmove(HtAddress,secondBlockData, strlen(secondBlockData));
    HashTable * hashTablePtr;
    sscanf(HtAddress,"%p",&hashTablePtr);

    /*We take the first GLOBAL-DEPTH bits from record.id*/

    return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
    //insert code here
    return HT_OK;
}

