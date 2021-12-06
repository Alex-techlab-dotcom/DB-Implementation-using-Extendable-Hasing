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
        OpenHashFiles[i].FileName = NULL;
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

    char *firstBlockData = BF_Block_GetData(firstBlock);

    memmove(firstBlockData, "HF", strlen("HF"));
    memcpy(firstBlockData + strlen("HF") * sizeof(char), &depth, sizeof(int));
    BF_Block_SetDirty(firstBlock);

    //char* blockData;
    char *secondBlockData = BF_Block_GetData(secondBlock);
    int sz = (int) pow(2, depth);
    int numOfEntries = 0;
    for ( int i = 0; i < sz; ++i ) {
        BF_Block *block;
        BF_Block_Init(&block);
        CALL_BF(BF_AllocateBlock(blockFileID, block));
        char *b_data = BF_Block_GetData(block);
        memcpy(b_data, &depth, sizeof(int));
        memcpy(b_data + sizeof(int), &numOfEntries, sizeof(int));
        BF_Block_SetDirty(block);
        int b = i + 2;
        memcpy(secondBlockData + (2 * i) * sizeof(int), &i, sizeof(int));
        memcpy(secondBlockData + (2 * i) * sizeof(int) + sizeof(int), &b, sizeof(int));

    }

    BF_Block_SetDirty(secondBlock);
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
    BF_Block *firstBlock, *secondBlock;
    char *firstBlockData, *secondBlockData;
    char HtAddress[20];
    int fileDesc = OpenHashFiles[indexDesc].BFid;
    int numOfEntries;

    CALL_BF(BF_GetBlock(fileDesc, 1, firstBlock));
    firstBlockData = BF_Block_GetData(firstBlock);

    char gd[10];
    int global_depth;
    memcpy(&global_depth, firstBlockData + strlen("HF"), sizeof(int));


    /* we retrieve the address of the hashtable from the second block*/
    CALL_BF(BF_GetBlock(fileDesc, 2, secondBlock));
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
    int destinationBlock;
    char *blockToWritePointer;

    memcpy(&destinationBlock, secondBlockData + (2 * block_index + 1) * sizeof(int), sizeof(int));
    /*
     * we retrieve the block at the blockindex(3rd index) and we name it blockToWrite
     * */
    BF_Block *blockToWrite;
    CALL_BF(BF_GetBlock(fileDesc, destinationBlock, blockToWrite));
    /*insertion of data*/
    blockToWritePointer = BF_Block_GetData(blockToWrite);
    /* first we check how many entries have already took place */
    memcpy(&numOfEntries, blockToWritePointer + sizeof(int), sizeof(int));
    if ( numOfEntries < (BF_BLOCK_SIZE - 2 * sizeof(int) / sizeof(Record))) {

        /* There is enough space so we make one more insertion*/
        numOfEntries++;
        memcpy(blockToWritePointer + sizeof(int), &numOfEntries, sizeof(int));

        //writing record to block
        /*
         * lockToWritePointer + 2 * sizeof(int) we leave 8 bytes at the beginning of the block
         * 4bytes for the LOCALDEPTH and 4 bytes for the number of entries
         */
        memcpy(blockToWritePointer + 2 * sizeof(int) + (numOfEntries - 1) * sizeof(Record), &record, sizeof(Record));


    } else {
        //increase depth
        global_depth++;
        memcpy(&global_depth, firstBlockData + strlen("HF"), sizeof(int));
        for ( int i = (int) pow(2, global_depth - 1); i < (int) pow(2, global_depth); i++ ) {
            memcpy(secondBlockData + (2 * i) * sizeof(int), &i, sizeof(int));//indexing
        }

        /**/
        for ( int i = (int) pow(2, global_depth) - 1; i >= 0; i-- ) {
            memcpy(secondBlockData + (2 * i) * sizeof(int) + sizeof(int),
                   secondBlockData + (2 * (i / 2)) * sizeof(int) + sizeof(int), sizeof(int));
        }

        /*split the bucket*/
        BF_Block *newBlock;
        int lastBlock;
        BF_Block_Init(&newBlock);
        CALL_BF(BF_AllocateBlock(fileDesc, newBlock));
        CALL_BF(BF_GetBlockCounter(fileDesc, &lastBlock));
        lastBlock--;

        /* we update the local depth of the block */
        int newLocalDepth;
        memcpy(&newLocalDepth,blockToWritePointer,sizeof(int ));
        newLocalDepth++;
        memcpy(blockToWritePointer,&newLocalDepth,sizeof(int ));

        /* we update the local depth of the new block as well */
        char *newBlockData = BF_Block_GetData(newBlock);
        memcpy(newBlockData, &newLocalDepth, sizeof(int));
        memcpy(&global_depth, blockToWritePointer, sizeof(int));

        /* we find all the pointers the show to the overflow block*/
        int counter = 0, bb, flag = 0, lastIndex;
        for ( int i = 0; i < (int) pow(2, global_depth); i++ ) {
            memcpy(&bb, secondBlockData + (2 * i) * sizeof(int) + sizeof(int), sizeof(int));
            if ( bb == destinationBlock ) {
                flag = 1;
                counter++;
            } else if ( flag ) {
                memcpy(&lastIndex, secondBlockData + (2 * (i - 1)) * sizeof(int), sizeof(int));
                break;
            }
        }

        for ( int i = 0; i < counter / 2; ++i ) {
            memcpy(secondBlockData + (2 * lastIndex) * sizeof(int) + sizeof(int), &lastBlock, sizeof(int));
            lastIndex--;
        }
        /*we reinsert the records of the overflow-block*/
        Record *recordArray[9];
        for ( int i = 0; i < 8; ++i ) {
            recordArray[i] = malloc(sizeof(Record));
            memcpy(blockToWritePointer + 2 * sizeof(int) + i * sizeof(Record), recordArray[i], sizeof(Record));
        }
        recordArray[8]= malloc(sizeof(Record));
        recordArray[8]=&record;
        //we keep only the local depth and we eraze everything else from the overflow block
        memset(blockToWritePointer + sizeof(int), 0, BF_BLOCK_SIZE -sizeof(int));


        numOfEntries = 0;
        for ( int i = 0; i < 9; ++i ) {
            rec_id = recordArray[i]->id;
            block_index = 0;
            //we retrieve the first global depth bits
            for ( int i = 0; i < global_depth; i++ ) {
                block_index += (rec_id % 2) * (int) pow(2, global_depth - i - 1);
                rec_id /= 2;
            }
            int newDestinationBlock;
            char *newBlockToWritePointer;

            memcpy(&newDestinationBlock, secondBlockData + (2 * block_index + 1) * sizeof(int), sizeof(int));
            BF_Block *blockToWrite;
            CALL_BF(BF_GetBlock(fileDesc, newDestinationBlock, blockToWrite));

            /*insertion of data*/
            newBlockToWritePointer = BF_Block_GetData(blockToWrite);
            //writing record to block
            memcpy(&numOfEntries, newBlockToWritePointer + sizeof(int), sizeof(int));
            numOfEntries++;
            memcpy(newBlockToWritePointer + 2 * sizeof(int) + (numOfEntries - 1) * sizeof(Record), &record,
                   sizeof(Record));

            memcpy(newBlockToWritePointer + sizeof(int), &numOfEntries, sizeof(int));
        }

    }


    return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
    //insert code here
    return HT_OK;
}

