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

HT_ErrorCode
SplitThePointers(int *pointersArray, int destinationBlock, int global_depth, int fileDesc, BF_Block *blockToWrite);

HT_ErrorCode Resize(int indexDesc, int destinationBlock, BF_Block *blockToWrite);

HT_ErrorCode HT_Init() {
    //insert code here
    for ( int i = 0; i < MAX_OPEN_FILES; ++i ) {
        OpenHashFiles[i].FileName = NULL;
    }
    BF_Init(MRU);
    return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {

    int blockFileID;
    // CALL_BF(BF_Init(LRU));//it uses the LRU method to clear the unneeded blocks from the buffer
    CALL_BF(BF_CreateFile(filename));
    CALL_BF(BF_OpenFile(filename, &blockFileID));// it gives the data1.db file a specific ID(lets say ID=11)

    /* Block Initialization*/
    BF_Block *firstBlock, *secondBlock;
    BF_Block_Init(&firstBlock);// it allocates the suitable space in memory
    BF_Block_Init(&secondBlock);// it allocates the suitable space in memory

    /* we allocate the blocks at the end of the BF_Block file */
    CALL_BF(BF_AllocateBlock(blockFileID, firstBlock));
    CALL_BF(BF_AllocateBlock(blockFileID, secondBlock));

    char *firstBlockData = BF_Block_GetData(firstBlock);
    int two = 1;
    char* str="HF";
    strcpy(firstBlockData, str);

    char* hf= malloc(strlen("HF")+1);
    memcpy(hf,firstBlockData,strlen("HF")+1);
    printf("sos: %s\n",hf);

    int debug;
    memcpy(firstBlockData + strlen("HF")+1, &depth, sizeof(int));
   // memcpy(&debug,firstBlockData + strlen("HF")+1,sizeof(int ));
   // printf("debug is %d\n",debug);
    memcpy(firstBlockData + strlen("HF")+1 + sizeof(int), &two, sizeof(int));


    //char* blockData;
    char *secondBlockData = BF_Block_GetData(secondBlock);
    int sz = (int) pow(2, depth);
    int numOfEntries = 0;
    for ( int offset = 0; offset < sz; ++offset ) {
        BF_Block *block;
        BF_Block_Init(&block);
        CALL_BF(BF_AllocateBlock(blockFileID, block));
        char *b_data = BF_Block_GetData(block);
        memcpy(b_data, &depth, sizeof(int));
        memcpy(b_data + sizeof(int), &numOfEntries, sizeof(int));
        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));
        int b = offset + 2;
        memcpy(secondBlockData + offset * sizeof(int), &b, sizeof(int));
        //  memcpy(secondBlockData + (2 * i) * sizeof(int) + sizeof(int), &b, sizeof(int));

    }

    BF_Block_SetDirty(secondBlock);
    BF_Block_SetDirty(firstBlock);
    CALL_BF(BF_UnpinBlock(secondBlock));
    CALL_BF(BF_UnpinBlock(firstBlock));
    return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc) {
    //insert code here
    for ( int i = 0; i < MAX_OPEN_FILES; ++i ) {
        if ( OpenHashFiles[i].FileName == NULL ) {
            OpenHashFiles[i].FileName = malloc((strlen(fileName) + 1) * sizeof(char));
            strcpy(OpenHashFiles[i].FileName, fileName);
            CALL_BF(BF_OpenFile(fileName, &OpenHashFiles[i].BFid));
            *indexDesc = i;
            return HT_OK;
        }
    }
    printf("NO SPACE AVAILABLE FOR HASHFILE :%s\n", fileName);
    return HT_ERROR;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
    //insert code here
    if ( indexDesc < 0 || indexDesc > MAX_OPEN_FILES ) {
        printf("INDEX OUT OF TABLE LENGTH\n");
        return HT_ERROR;
    }
    if ( OpenHashFiles[indexDesc].FileName == NULL )printf("THERE IS NO OPEN FILE AT THIS INDEX\n");
    CALL_BF(BF_CloseFile(OpenHashFiles[indexDesc].BFid));
    free(OpenHashFiles[indexDesc].FileName);
    OpenHashFiles[indexDesc].FileName = NULL;
    return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
    //insert code here
    //printf("INSIDE HT_ENTRY\n");
    BF_Block *firstBlock, *secondBlock;
    char *firstBlockData, *secondBlockData;
    char HtAddress[20];
    int fileDesc = OpenHashFiles[indexDesc].BFid;
   // printf("Bfid:%d\n",fileDesc);
    int numOfEntries;

    BF_Block_Init(&firstBlock);// it allocates the suitable space in memory
    BF_Block_Init(&secondBlock);// it allocates the suitable space in memory

    CALL_BF(BF_GetBlock(fileDesc, 0, firstBlock));
    firstBlockData = BF_Block_GetData(firstBlock);


    int global_depth;
    memcpy(&global_depth, firstBlockData + strlen("HF")+1, sizeof(int));


    /* we retrieve the address of the hashtable from the second block*/
    CALL_BF(BF_GetBlock(fileDesc, 1, secondBlock));
    secondBlockData = BF_Block_GetData(secondBlock);

    //hash function
    int rec_id = record.id;
    int block_index = 0;
    for ( int i = 0; i < global_depth; i++ ) {
        block_index += (rec_id % 2) * (int) pow(2, global_depth - i - 1);
        rec_id /= 2;
    }
    /*
     * block_index is the decimal representation of the first-global_depth bits of the record id
     * lets assume that ID=7(111) and global_depth=2 so block_index=3(11)
     */
    int hashBlockIndex;
    memcpy(&hashBlockIndex, firstBlockData + strlen("HF")+1 + sizeof(int) + (block_index / 128) * sizeof(int), sizeof(int));
    //printf("hashblockIndex inside insert is %d\n",hashBlockIndex);
    //we find the corresponding hashblock
    BF_Block *hashBlock;
    char *hashBlockData;
    BF_Block_Init(&hashBlock);
    CALL_BF(BF_GetBlock(fileDesc, hashBlockIndex, hashBlock));
    hashBlockData = BF_Block_GetData(hashBlock);


    int destinationBlock;
    char *blockToWritePointer;

    memcpy(&destinationBlock, hashBlockData + (block_index % 128) * sizeof(int), sizeof(int));

    /*
     * we retrieve the block at the blockindex(3rd index) and we name it blockToWrite
     * */
    BF_Block *blockToWrite;
    BF_Block_Init(&blockToWrite);// it allocates the suitable space in memory
    CALL_BF(BF_GetBlock(fileDesc, destinationBlock, blockToWrite));
    /*insertion of data*/
    blockToWritePointer = BF_Block_GetData(blockToWrite);
    /* first we check how many entries have already took place */
    memcpy(&numOfEntries, blockToWritePointer + sizeof(int), sizeof(int));
    if ( numOfEntries < (BF_BLOCK_SIZE - 2 * sizeof(int) )/ sizeof(Record)) {

        /* There is enough space so we make one more insertion*/
        numOfEntries++;
        memcpy(blockToWritePointer + sizeof(int), &numOfEntries, sizeof(int));

        //writing record to block
        /*
         * blockToWritePointer + 2 * sizeof(int) we leave 8 bytes at the beginning of the block
         * 4bytes for the LOCALDEPTH and 4 bytes for the number of entries
         */
        memcpy(blockToWritePointer + 2 * sizeof(int) + (numOfEntries - 1) * sizeof(Record), &record, sizeof(Record));


    } else {

        printf("destination block %d that has more than 8 entries\n",destinationBlock);
        printf("global depth before resize:%d\n",global_depth);
        Resize(indexDesc, destinationBlock, blockToWrite);
        firstBlockData = BF_Block_GetData(firstBlock);
        memcpy(&global_depth,firstBlockData+ strlen("HF")+1, sizeof(int));
        printf("new global depth after resize:%d\n",global_depth);


        printf("BEFORE REINSERTION\n");
       // printEverything(fileDesc);
        printf("\n\n\n\n");

        /*we reinsert the records of the overflow-block*/
        printf("we reinsert the records of the overflow-block\n");
        Record *recordArray[9];
        for ( int i = 0; i < 8; ++i ) {
            recordArray[i] = malloc(sizeof(Record));
            memcpy(recordArray[i],blockToWritePointer + 2 * sizeof(int) + i * sizeof(Record), sizeof(Record));
        }
        recordArray[8] = malloc(sizeof(Record));
        recordArray[8] = &record;
        //we keep only the local depth and we eraze everything else from the overflow block
        memset(blockToWritePointer + sizeof(int), 0, BF_BLOCK_SIZE - sizeof(int));

       // printf("255\n");
        numOfEntries = 0;
        BF_Block *newBlockToWrite;
        BF_Block_Init(&newBlockToWrite);
        for ( int i = 0; i < 9; ++i ) {
            rec_id = recordArray[i]->id;
            //printf("id =%d\n",rec_id);
            block_index = 0;
            //we retrieve the first global depth bits
            for ( int i = 0; i < global_depth; i++ ) {
                block_index += (rec_id % 2) * (int) pow(2, global_depth - i - 1);
                rec_id /= 2;
            }
            printf("blockindex:%d\n",block_index);
            int newDestinationBlock;
            char *newBlockToWritePointer;



            int hashBlockIndex;

            memcpy(&hashBlockIndex, firstBlockData + strlen("HF")+1 + sizeof(int) + block_index / 128 * sizeof(int), sizeof(int));


            //we find the corresponding hashblock
            // printf("hasblockindex = %d\n",hashBlockIndex);
            CALL_BF(BF_GetBlock(fileDesc, hashBlockIndex, hashBlock));



            hashBlockData= BF_Block_GetData(hashBlock);
            memcpy(&newDestinationBlock, hashBlockData + (block_index%128) * sizeof(int), sizeof(int));
            printf("newDestinationBlock:%d\n",newDestinationBlock);
            printf("hasblockIndex:%d\n",hashBlockIndex);
            CALL_BF(BF_GetBlock(fileDesc, newDestinationBlock, newBlockToWrite));


            /*insertion of data*/
            newBlockToWritePointer = BF_Block_GetData(newBlockToWrite);
            //writing record to block
            memcpy(&numOfEntries, newBlockToWritePointer + sizeof(int), sizeof(int));
            numOfEntries++;
            memcpy(newBlockToWritePointer + 2 * sizeof(int) + (numOfEntries - 1) * sizeof(Record), recordArray[i],
                   sizeof(Record));

            memcpy(newBlockToWritePointer + sizeof(int), &numOfEntries, sizeof(int));
            BF_Block_SetDirty(newBlockToWrite);
            CALL_BF(BF_UnpinBlock(newBlockToWrite));
        }


        for ( int i = 0; i <9 ; ++i ) {
          //  free(recordArray[i]);
        }

    }

    BF_Block_SetDirty(secondBlock);
    BF_Block_SetDirty(firstBlock);
    CALL_BF(BF_UnpinBlock(secondBlock));
    CALL_BF(BF_UnpinBlock(firstBlock));
    //printf("EXITING HT_ENTRY\n");

    return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
    //printf("INSIDE PRINT\n");
    int fileDesc = OpenHashFiles[indexDesc].BFid;
   // printf("Bfid:%d\n",fileDesc);
    int block_index = 0;
    int rec_id = *id;
    char *firstBlockData, *secondBlockData;
    BF_Block *firstBlock, *secondBlock;
    int destinationBlock;

    BF_Block_Init(&firstBlock);// it allocates the suitable space in memory
    BF_Block_Init(&secondBlock);// it allocates the suitable space in memory

    CALL_BF(BF_GetBlock(fileDesc, 0, firstBlock));
    firstBlockData = BF_Block_GetData(firstBlock);


    int global_depth;
    memcpy(&global_depth, firstBlockData + strlen("HF")+1, sizeof(int));
   // printf("317\n");

    /* we retrieve the address of the hashtable from the second block*/
    CALL_BF(BF_GetBlock(fileDesc, 1, secondBlock));
    secondBlockData = BF_Block_GetData(secondBlock);

    for ( int i = 0; i < global_depth; i++ ) {
        block_index += (rec_id % 2) * (int) pow(2, global_depth - i - 1);
        rec_id /= 2;
    }

    int hashBlockIndex;
    memcpy(&hashBlockIndex, firstBlockData + strlen("HF")+1 + sizeof(int) + block_index / 128 * sizeof(int), sizeof(int));
   // printf("330\n");
    //char* hf= malloc(strlen("HF")+1);
    //strcpy(hf,firstBlockData);
   // printf("sos:%s\n",hf);

    //we find the corresponding hashblock
    BF_Block *hashBlock;
    char *hashBlockData;
    BF_Block_Init(&hashBlock);
   // printf("hasblockindex = %d\n",hashBlockIndex);
    CALL_BF(BF_GetBlock(fileDesc, hashBlockIndex, hashBlock));
    hashBlockData = BF_Block_GetData(hashBlock);

    memcpy(&destinationBlock, hashBlockData + (block_index % 128) * sizeof(int), sizeof(int));
    //printf("340\n");
    //we find the right block inside the hashblock
    BF_Block *blockForPrint;

    char *blockForPrintdata;
    BF_Block_Init(&blockForPrint);
    CALL_BF(BF_GetBlock(fileDesc, destinationBlock, blockForPrint));
    blockForPrintdata= BF_Block_GetData(blockForPrint);
    int bucketEntries;
    //coredump!!!!!!
    memcpy(&bucketEntries, blockForPrintdata + sizeof(int), sizeof(int));
    //printf("350\n");

    Record *bucketRecords = malloc(bucketEntries * sizeof(Record));
    memcpy(bucketRecords, blockForPrintdata + 2 * sizeof(int), bucketEntries * sizeof(Record));

    for ( int i = 0; i < bucketEntries; ++i ) {
        if ( bucketRecords[i].id == (*id)) {
            printf("Hash Block: %d || Bucket Number: %d || ID: %d || Name: %s || Surname: %s || City: %s\n",
                   hashBlockIndex, destinationBlock, bucketRecords[i].id, bucketRecords[i].name,
                   bucketRecords[i].surname, bucketRecords[i].city);
            break;
        }
    }

    free(bucketRecords);
   // printf("EXITING PRINT\n");
    CALL_BF(BF_UnpinBlock(secondBlock));
    CALL_BF(BF_UnpinBlock(firstBlock));
    CALL_BF(BF_UnpinBlock(hashBlock));
    CALL_BF(BF_UnpinBlock(blockForPrint));
    return HT_OK;
}

HT_ErrorCode Resize(int indexDesc, int destinationBlock, BF_Block *blockToWrite) {
    printf("RESIZE\n");

    int fileDesc = OpenHashFiles[indexDesc].BFid;
    char *firstBlockData, *secondBlockData, *blockData;
    char HtAddress[20];
    int numOfEntries;
    BF_Block *firstBlock, *secondBlock, *blockPtr;
    int maxIntsEntries = BF_BLOCK_SIZE / sizeof(int);

    BF_Block_Init(&firstBlock);// it allocates the suitable space in memory
    BF_Block_Init(&secondBlock);// it allocates the suitable space in memory
    BF_Block_Init(&blockPtr);// it allocates the suitable space in memory
    /* we retrieve and update the global depth from block one */
    CALL_BF(BF_GetBlock(fileDesc, 0, firstBlock));
    firstBlockData = BF_Block_GetData(firstBlock);
    int global_depth;
    memcpy(&global_depth, firstBlockData + strlen("HF")+1, sizeof(int));
    global_depth++;
    memcpy(firstBlockData + strlen("HF") +1, &global_depth, sizeof(int));
    BF_Block_SetDirty(firstBlock);
    CALL_BF(BF_UnpinBlock(firstBlock));
    printf("global in resize %d\n",global_depth);

    int size = (int) pow(2, global_depth);
    int *PointersArray = malloc(size * sizeof(int));
    if ( global_depth <= 7 ) {
        printf("one block for hashtable\n");
        //one block for hashtable
        CALL_BF(BF_GetBlock(fileDesc, 1, secondBlock));
        char *secondBlockData = BF_Block_GetData(secondBlock);
        memcpy(PointersArray, secondBlockData, (int) pow(2, global_depth - 1) * sizeof(int));

        //we change the pointers;
        for ( int i = (size - 1); i >= 0; i-- ) {
            PointersArray[i] = PointersArray[i / 2];
        }

      /*  printf("before split\n");
        for ( int i = 0; i < size; ++i ) {
            int value;
            printf("%d\n", PointersArray[i]);
        }*/
        SplitThePointers(PointersArray, destinationBlock, global_depth, fileDesc, blockToWrite);

        //we copy the pointers from the array back to the secondBlock;
        memcpy(secondBlockData, PointersArray, size * sizeof(int));
        printf("after split\n");
      /*  for ( int i = 0; i < size; ++i ) {
            int value;
            memcpy(&value,secondBlockData+i*sizeof(int),sizeof(int));
            printf("value :%d\n",value);
        }
        printf("blockToWrite = %p\n",(void *)blockToWrite);
        */
        return HT_OK;
    } else {


        int blockNumber;
        int hashBlocks = (int) (pow(2, global_depth - 1) / maxIntsEntries);
        // we copy the already existed hashblocks back to array
        for ( int i = 0; i < hashBlocks; ++i ) {

            memcpy(&blockNumber, firstBlockData + strlen("HF")+1 + sizeof(int) + i * sizeof(int), sizeof(int));
            CALL_BF(BF_GetBlock(fileDesc, blockNumber, blockPtr));
            blockData = BF_Block_GetData(blockPtr);
            memcpy(PointersArray + (i) * sizeof(int) * maxIntsEntries, blockData, maxIntsEntries * sizeof(int));
           // CALL_BF(BF_UnpinBlock(blockPtr));

        }

        //we change the pointers;
        for ( int i = (int) pow(2, global_depth) - 1; i >= 0; i-- ) {
            PointersArray[i] = PointersArray[i / 2];
        }

        SplitThePointers(PointersArray, destinationBlock, global_depth, fileDesc, blockToWrite);


        int newBlocks = ((int) pow(2, global_depth) / maxIntsEntries);
        for ( int i = hashBlocks; i < newBlocks; ++i ) {
            BF_Block *newBlock;
            BF_Block_Init(&newBlock);// it allocates the suitable space in memory
            /* we allocate the blocks at the end of the BF_Block file */
            CALL_BF(BF_AllocateBlock(fileDesc, newBlock));
            CALL_BF(BF_GetBlockCounter(fileDesc, &blockNumber));
            blockNumber--;
            //we take the number of the last-allocated block;
            memcpy(firstBlockData + strlen("HF") +1+ sizeof(int) + i * sizeof(int), &blockNumber, sizeof(int));
            // BF_Block_SetDirty(newBlock);
            //CALL_BF(BF_UnpinBlock(newBlock));
        }

        for ( int i = 0; i < newBlocks; ++i ) {
            memcpy(&blockNumber, firstBlockData + strlen("HF")+1 + sizeof(int) + i * sizeof(int), sizeof(int));
            BF_Block *newBlockToWrite;
            BF_Block_Init(&newBlockToWrite);// it allocates the suitable space in memory
            CALL_BF(BF_GetBlock(fileDesc, blockNumber, newBlockToWrite));
            blockData = BF_Block_GetData(newBlockToWrite);
            memcpy(blockData, PointersArray + i * maxIntsEntries, maxIntsEntries * sizeof(int));
            BF_Block_SetDirty(newBlockToWrite);
            CALL_BF(BF_UnpinBlock(newBlockToWrite));
        }


    }
    printf("blockToWrite = %p\n",(void *)blockToWrite);
    BF_Block_SetDirty(firstBlock);
    CALL_BF(BF_UnpinBlock(firstBlock));
    BF_Block_SetDirty(secondBlock);
    CALL_BF(BF_UnpinBlock(secondBlock));
    return HT_OK;

}

HT_ErrorCode
SplitThePointers(int *pointersArray, int destinationBlock, int global_depth, int fileDesc, BF_Block *blockToWrite) {


    char *blockToWritePointer = BF_Block_GetData(blockToWrite);

    BF_Block *newBlock;
    int lastBlock;
    BF_Block_Init(&newBlock);
    CALL_BF(BF_AllocateBlock(fileDesc, newBlock));
    CALL_BF(BF_GetBlockCounter(fileDesc, &lastBlock));
    lastBlock--;
   // printf("the new block id is %d\n",lastBlock);

    int newLocalDepth;
    memcpy(&newLocalDepth, blockToWritePointer, sizeof(int));
    newLocalDepth++;
    memcpy(blockToWritePointer, &newLocalDepth, sizeof(int));


    /* we update the local depth of the new block as well */
    char *newBlockData = BF_Block_GetData(newBlock);
    memcpy(newBlockData, &newLocalDepth, sizeof(int));
    //memcpy(&global_depth, blockToWritePointer, sizeof(int));
   // printf("destinationblock in splitpointers %d\n",destinationBlock);
    int counter = 0, bb, flag = 0, lastIndex=-1;
    //printf("GLOBAL DEPTH IN SPLIT %d\n",global_depth);
    int size=(int) pow(2, global_depth);
    for ( int i = 0; i < size; i++ ) {
        //printf("pointersArray[%d]=%d\n",i,pointersArray[i]);
        if ( pointersArray[i] == destinationBlock ) {
          //  printf("index %d\n",i);
            flag = 1;
            counter++;
        } else if ( flag ) {

            lastIndex = i - 1;

            break;
        }
    }
    if(lastIndex==-1)lastIndex=size-1;
    for ( int i = 0; i < counter / 2; ++i ) {
       // printf("lastindex=%d\n",lastIndex);
        pointersArray[lastIndex] = lastBlock;
        lastIndex--;
    }

    BF_Block_SetDirty(newBlock);
    CALL_BF(BF_UnpinBlock(newBlock));
    BF_Block_SetDirty(blockToWrite);
    CALL_BF(BF_UnpinBlock(blockToWrite));

    return HT_OK;
}
HT_ErrorCode printEverything(int indexDesc){
    int fileDesc = OpenHashFiles[indexDesc].BFid;
    int uniqueBlocks;
    BF_GetBlockCounter(fileDesc,&uniqueBlocks);
    BF_Block * blockPtr;
    BF_Block_Init(&blockPtr);
    char* blockPtrData;
    for ( int i = 2; i <uniqueBlocks ; ++i ) {
        BF_GetBlock(fileDesc,i,blockPtr);
        blockPtrData= BF_Block_GetData(blockPtr);
        int entries;
        memcpy(&entries,blockPtrData+ sizeof(int), sizeof(int));
        Record r;
        for ( int j = 0; j < entries; ++j ) {
            memcpy(&r,blockPtrData+2* sizeof(int)+j* sizeof(Record), sizeof(Record));
            printf("Hash Block: %d || Bucket Number: %d || ID: %d || Name: %s || Surname: %s || City: %s\n",
                   1, i, r.id, r.name,
                   r.surname, r.city);
        }
        CALL_BF(BF_UnpinBlock(blockPtr));
    }
    return HT_OK;
}
