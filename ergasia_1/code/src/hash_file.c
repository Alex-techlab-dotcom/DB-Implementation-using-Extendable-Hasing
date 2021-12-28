#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include "../include/bf.h"
#include "../include/hash_file.h"



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

HT_ErrorCode CopyBlocksToArray(int *arrayOfPointers, int fileDesc, int global_depth);

HT_ErrorCode CopyArrayToBlocks(int *arrayOfPointers, int global_depth, int fileDesc);

HT_ErrorCode Resize(int indexDesc, int destinationBlock, BF_Block *blockToWrite);

HT_ErrorCode HT_Init() {
    BF_Init(LRU);
    for ( int i = 0; i < MAX_OPEN_FILES; ++i ) {
        OpenHashFiles[i].FileName = NULL;
    }
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
    char *str = "HF";
    strcpy(firstBlockData, str);

    char *hf = malloc(strlen("HF") + 1);
    memcpy(hf, firstBlockData, strlen("HF") + 1);
    free(hf);
    memcpy(firstBlockData + strlen("HF") + 1, &depth, sizeof(int));
    memcpy(firstBlockData + strlen("HF") + 1 + sizeof(int), &two, sizeof(int));


    char *secondBlockData = BF_Block_GetData(secondBlock);
    int sz = (int) pow(2, depth);
    int numOfEntries = 0;
    /*stin arxi se ka8e mplok deixnei enas deiktis (1 = 2^0) opote to ka8e mplok exei local_d=global_d-0 => local_d=global_d*/
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
        BF_Block_Destroy(&block);
    }

    BF_Block_SetDirty(secondBlock);
    BF_Block_SetDirty(firstBlock);
    CALL_BF(BF_UnpinBlock(secondBlock));
    CALL_BF(BF_UnpinBlock(firstBlock));
    BF_Block_Destroy(&secondBlock);
    BF_Block_Destroy(&firstBlock);
    return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc) {
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

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record, int *tupleId, UpdateRecordArray *updateArray) {
    BF_Block *firstBlock, *secondBlock;
    char *firstBlockData, *secondBlockData;
    char HtAddress[20];
    int fileDesc = OpenHashFiles[indexDesc].BFid;
    int numOfEntries;

    BF_Block_Init(&firstBlock);// it allocates the suitable space in memory
    BF_Block_Init(&secondBlock);// it allocates the suitable space in memory

    CALL_BF(BF_GetBlock(fileDesc, 0, firstBlock));
    firstBlockData = BF_Block_GetData(firstBlock);


    int global_depth;
    memcpy(&global_depth, firstBlockData + strlen("HF") + 1, sizeof(int));


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
    memcpy(&hashBlockIndex, firstBlockData + strlen("HF") + 1 + sizeof(int) +
                            (block_index / (BF_BLOCK_SIZE / sizeof(int))) * sizeof(int),
           sizeof(int));
    //we find the corresponding hashblock
    BF_Block *hashBlock;
    char *hashBlockData;
    BF_Block_Init(&hashBlock);
    CALL_BF(BF_GetBlock(fileDesc, hashBlockIndex, hashBlock));
    hashBlockData = BF_Block_GetData(hashBlock);


    int destinationBlock;
    char *blockToWritePointer;

    memcpy(&destinationBlock, hashBlockData + (block_index % (BF_BLOCK_SIZE / sizeof(int))) * sizeof(int), sizeof(int));

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
    BF_UnpinBlock(hashBlock);
    if ( numOfEntries < (BF_BLOCK_SIZE - 2 * sizeof(int)) / sizeof(Record)) {

        /* There is enough space so we make one more insertion*/
        numOfEntries++;
        memcpy(blockToWritePointer + sizeof(int), &numOfEntries, sizeof(int));

        //writing record to block
        /*
         * blockToWritePointer + 2 * sizeof(int) we leave 8 bytes at the beginning of the block
         * 4bytes for the LOCALDEPTH and 4 bytes for the number of entries
         */
        memcpy(blockToWritePointer + 2 * sizeof(int) + (numOfEntries - 1) * sizeof(Record), &record, sizeof(Record));

        *tupleId = ((destinationBlock + 1) * (BF_BLOCK_SIZE - 2 * sizeof(int)) / sizeof(Record)) + numOfEntries;
        //updateArray = NULL; // IT SIGNALS THAT NO SPLIT WAS NEEDED

        updateArray->hasResults=0;

        BF_Block_SetDirty(blockToWrite);
        CALL_BF(BF_UnpinBlock(blockToWrite));

    } else {

        //MORE THAN 8 ENTRIES INSIDE THE BLOCK
        //blockToWrite is the block that has already 8 entries
        int local_depth;
        memcpy(&local_depth, blockToWritePointer, sizeof(int));

        if ( local_depth == global_depth ) {
            //resize and split
            CALL_OR_DIE(Resize(indexDesc, destinationBlock, blockToWrite));
        } else if ( local_depth < global_depth ) {
            //split
            int *array = malloc(((int) pow(2, global_depth)) * sizeof(int));
            CALL_OR_DIE(CopyBlocksToArray(array, fileDesc, global_depth));
            CALL_OR_DIE(SplitThePointers(array, destinationBlock, global_depth, fileDesc, blockToWrite));
            CALL_OR_DIE(CopyArrayToBlocks(array, global_depth, fileDesc));
            free(array);
        }

        firstBlockData = BF_Block_GetData(firstBlock);
        memcpy(&global_depth, firstBlockData + strlen("HF") + 1, sizeof(int));

        /*we reinsert the records of the overflow-block*/
        updateArray->hasResults=1;
        Record *recordArray[9];
        for ( int i = 0; i < 8; ++i ) {
            recordArray[i] = malloc(sizeof(Record));
            memcpy(recordArray[i], blockToWritePointer + 2 * sizeof(int) + i * sizeof(Record), sizeof(Record));
           // updateArray[i]->recordPointer = malloc(sizeof(Record));
            updateArray->array[i].recordPointer=recordArray[i];
        }
        //recordArray[8] = malloc(sizeof(Record));
        recordArray[8] = &record;
        //we keep only the local depth and we eraze everything else from the overflow block
        memset(blockToWritePointer + sizeof(int), 0, BF_BLOCK_SIZE - sizeof(int));
        int zero = 0;
        memcpy(blockToWritePointer + sizeof(int), &zero, sizeof(int));

        numOfEntries = 0;
        BF_Block *newBlockToWrite;
        BF_Block_Init(&newBlockToWrite);
        BF_Block_SetDirty(blockToWrite);
        BF_UnpinBlock(blockToWrite);
        for ( int i = 0; i < 9; ++i ) {
            rec_id = recordArray[i]->id;
            //printf("recId %d\n",rec_id);
            block_index = 0;
            //we retrieve the first global depth bits
            for ( int i = 0; i < global_depth; i++ ) {
                block_index += (rec_id % 2) * (int) pow(2, global_depth - i - 1);
                rec_id /= 2;
            }
            int newDestinationBlock;
            char *newBlockToWritePointer;


            int hashBlockIndex;

            memcpy(&hashBlockIndex, firstBlockData + strlen("HF") + 1 + sizeof(int) +
                                    (block_index / (BF_BLOCK_SIZE / sizeof(int))) * sizeof(int),
                   sizeof(int));

            //we find the corresponding hashblock
            CALL_BF(BF_GetBlock(fileDesc, hashBlockIndex, hashBlock));

            hashBlockData = BF_Block_GetData(hashBlock);
            memcpy(&newDestinationBlock, hashBlockData + (block_index % (BF_BLOCK_SIZE / sizeof(int))) * sizeof(int),
                   sizeof(int));
            CALL_BF(BF_GetBlock(fileDesc, newDestinationBlock, newBlockToWrite));

            /*insertion of data*/
            newBlockToWritePointer = BF_Block_GetData(newBlockToWrite);
            //writing record to block
            memcpy(&numOfEntries, newBlockToWritePointer + sizeof(int), sizeof(int));
            numOfEntries++;
            if ( numOfEntries == 9 ) {
                printf("stop\n");
            }
            memcpy(newBlockToWritePointer + 2 * sizeof(int) + (numOfEntries - 1) * sizeof(Record), recordArray[i],
                   sizeof(Record));

            memcpy(newBlockToWritePointer + sizeof(int), &numOfEntries, sizeof(int));

            if ( i == 8 ) {
                *tupleId = ((newDestinationBlock + 1) * (BF_BLOCK_SIZE - 2 * sizeof(int)) / sizeof(Record)) + numOfEntries;
            }
            else{
                updateArray->array[i].position = numOfEntries;
                updateArray->array[i].bucketId = newDestinationBlock;
            }

            BF_Block_SetDirty(newBlockToWrite);
            CALL_BF(BF_UnpinBlock(newBlockToWrite));
            CALL_BF(BF_UnpinBlock(hashBlock));

        }

        BF_Block_Destroy(&newBlockToWrite);
        BF_Block_Destroy(&hashBlock);

        for ( int i = 0; i <8 ; ++i ) {
            free(recordArray[i]);
        }
    }

    BF_Block_SetDirty(secondBlock);
    BF_Block_SetDirty(firstBlock);
    CALL_BF(BF_UnpinBlock(secondBlock));
    CALL_BF(BF_UnpinBlock(firstBlock));
    BF_Block_Destroy(&firstBlock);
    BF_Block_Destroy(&secondBlock);
    BF_Block_Destroy(&blockToWrite);


    return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
    int fileDesc = OpenHashFiles[indexDesc].BFid;
    if ( id == NULL ) {

        CALL_OR_DIE(printEverything(indexDesc));
        return HT_OK;
    }
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
    memcpy(&global_depth, firstBlockData + strlen("HF") + 1, sizeof(int));

    /* we retrieve the address of the hashtable from the second block*/
    CALL_BF(BF_GetBlock(fileDesc, 1, secondBlock));
    secondBlockData = BF_Block_GetData(secondBlock);

    for ( int i = 0; i < global_depth; i++ ) {
        block_index += (rec_id % 2) * (int) pow(2, global_depth - i - 1);
        rec_id /= 2;
    }

    int hashBlockIndex;
    memcpy(&hashBlockIndex,
           firstBlockData + strlen("HF") + 1 + sizeof(int) + block_index / (BF_BLOCK_SIZE / sizeof(int)) * sizeof(int),
           sizeof(int));

    //we find the corresponding hashblock
    BF_Block *hashBlock;
    char *hashBlockData;
    BF_Block_Init(&hashBlock);
    CALL_BF(BF_GetBlock(fileDesc, hashBlockIndex, hashBlock));
    hashBlockData = BF_Block_GetData(hashBlock);

    memcpy(&destinationBlock, hashBlockData + (block_index % (BF_BLOCK_SIZE / sizeof(int))) * sizeof(int), sizeof(int));
    //we find the right block inside the hashblock
    BF_Block *blockForPrint;

    char *blockForPrintdata;
    BF_Block_Init(&blockForPrint);
    CALL_BF(BF_GetBlock(fileDesc, destinationBlock, blockForPrint));
    blockForPrintdata = BF_Block_GetData(blockForPrint);
    int bucketEntries;
    memcpy(&bucketEntries, blockForPrintdata + sizeof(int), sizeof(int));

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
    CALL_BF(BF_UnpinBlock(secondBlock));
    CALL_BF(BF_UnpinBlock(firstBlock));
    CALL_BF(BF_UnpinBlock(hashBlock));
    CALL_BF(BF_UnpinBlock(blockForPrint));
    BF_Block_Destroy(&secondBlock);
    BF_Block_Destroy(&firstBlock);
    BF_Block_Destroy(&hashBlock);
    BF_Block_Destroy(&blockForPrint);
    return HT_OK;
}

HT_ErrorCode Resize(int indexDesc, int destinationBlock, BF_Block *blockToWrite) {
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

    //we increase the global depth
    int global_depth;
    memcpy(&global_depth, firstBlockData + strlen("HF") + 1, sizeof(int));
    global_depth++;
    memcpy(firstBlockData + strlen("HF") + 1, &global_depth, sizeof(int));
    BF_Block_SetDirty(firstBlock);
    CALL_BF(BF_UnpinBlock(firstBlock));

    int size = (int) pow(2, global_depth);
    int *PointersArray = malloc(size * sizeof(int));
    if ( global_depth <= 7 ) {
        //one block for hashtable
        CALL_BF(BF_GetBlock(fileDesc, 1, secondBlock));
        char *secondBlockData = BF_Block_GetData(secondBlock);
        memcpy(PointersArray, secondBlockData, (int) pow(2, global_depth - 1) * sizeof(int));

        //we change the pointers;
        for ( int i = (size - 1); i >= 0; i-- ) {
            int index = (int) floorf(i / 2);
            PointersArray[i] = PointersArray[index];
        }

        CALL_OR_DIE(SplitThePointers(PointersArray, destinationBlock, global_depth, fileDesc, blockToWrite));

        //we copy the pointers from the array back to the secondBlock;
        memcpy(secondBlockData, PointersArray, size * sizeof(int));

        BF_Block_SetDirty(firstBlock);
        CALL_BF(BF_UnpinBlock(firstBlock));
        CALL_BF(BF_UnpinBlock(secondBlock));
        BF_Block_Destroy(&secondBlock);
        //BF_Block_Destroy(&firstBlock);
        BF_Block_Destroy(&blockPtr);
        return HT_OK;
    } else {

        int blockNumber;
        int hashBlocks = (int) (pow(2, global_depth - 1) / maxIntsEntries);
        // we copy the already existed hashblocks back to array
        for ( int i = 0; i < hashBlocks; ++i ) {

            memcpy(&blockNumber, firstBlockData + strlen("HF") + 1 + sizeof(int) + i * sizeof(int), sizeof(int));

            CALL_BF(BF_GetBlock(fileDesc, blockNumber, blockPtr));
            blockData = BF_Block_GetData(blockPtr);
            for ( int j = 0; j < (BF_BLOCK_SIZE / sizeof(int)); ++j ) {
                int v;
                memcpy(&v, blockData + j * sizeof(int), sizeof(int));
                PointersArray[j + i * maxIntsEntries] = v;
            }
            CALL_BF(BF_UnpinBlock(blockPtr));
        }

        //we change the pointers;
        for ( int i = (int) pow(2, global_depth) - 1; i >= 0; i-- ) {
            int index = (int) floorf(i / 2);
            PointersArray[i] = PointersArray[index];
        }


        CALL_OR_DIE(SplitThePointers(PointersArray, destinationBlock, global_depth, fileDesc, blockToWrite));

        int newBlocks = ((int) pow(2, global_depth) / maxIntsEntries);
        BF_Block *newBlock;
        BF_Block_Init(&newBlock);
        for ( int i = hashBlocks; i < newBlocks; ++i ) {
            // it allocates the suitable space in memory
            /* we allocate the blocks at the end of the BF_Block file */
            CALL_BF(BF_AllocateBlock(fileDesc, newBlock));
            CALL_BF(BF_GetBlockCounter(fileDesc, &blockNumber));
            blockNumber--;
            //we take the number of the last-allocated block;
            memcpy(firstBlockData + strlen("HF") + 1 + sizeof(int) + i * sizeof(int), &blockNumber, sizeof(int));
            CALL_BF(BF_UnpinBlock(newBlock));
        }
        BF_Block *newBlockToWrite;
        BF_Block_Init(&newBlockToWrite);
        for ( int i = 0; i < newBlocks; ++i ) {
            memcpy(&blockNumber, firstBlockData + strlen("HF") + 1 + sizeof(int) + i * sizeof(int), sizeof(int));
            // it allocates the suitable space in memory
            CALL_BF(BF_GetBlock(fileDesc, blockNumber, newBlockToWrite));
            blockData = BF_Block_GetData(newBlockToWrite);
            memcpy(blockData, PointersArray + i * maxIntsEntries, maxIntsEntries * sizeof(int));
            BF_Block_SetDirty(newBlockToWrite);
            CALL_BF(BF_UnpinBlock(newBlockToWrite));
        }
        BF_Block_Destroy(&newBlock);
        BF_Block_Destroy(&newBlockToWrite);
    }
    BF_Block_SetDirty(firstBlock);
    CALL_BF(BF_UnpinBlock(firstBlock));
    BF_Block_SetDirty(secondBlock);
    CALL_BF(BF_UnpinBlock(secondBlock));
    BF_Block_Destroy(&secondBlock);
    BF_Block_Destroy(&firstBlock);
    // BF_Block_Destroy(&firstBlock);
    free(PointersArray);
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
    char *newBlockData = BF_Block_GetData(newBlock);
    lastBlock--;

    int newLocalDepth;
    // we retrieve the local depth from the the block with the 8 entries
    memcpy(&newLocalDepth, blockToWritePointer, sizeof(int));
    newLocalDepth++;

    // we increase it by one and we write it back
    memcpy(blockToWritePointer, &newLocalDepth, sizeof(int));

    /* we update the local depth of the new block as well */
    memcpy(newBlockData, &newLocalDepth, sizeof(int));

    int counter = 0, flag = 0, lastIndex = -1;
    int size = (int) pow(2, global_depth);
    for ( int i = 0; i < size; i++ ) {
        if ( pointersArray[i] == destinationBlock ) {
            flag = 1;
            counter++;
        } else if ( flag ) {
            lastIndex = i - 1;
            break;
        }
    }
    if ( lastIndex == -1 )lastIndex = size - 1;
    for ( int i = 0; i < counter / 2; ++i ) {
        pointersArray[lastIndex] = lastBlock;
        lastIndex--;
    }
    BF_Block_SetDirty(newBlock);
    CALL_BF(BF_UnpinBlock(newBlock));
    BF_Block_Destroy(&newBlock);
    BF_Block_SetDirty(blockToWrite);


    return HT_OK;
}

HT_ErrorCode printEverything(int indexDesc) {
    int fileDesc = OpenHashFiles[indexDesc].BFid;

    BF_Block *firstBlock;
    BF_Block_Init(&firstBlock);
    CALL_BF(BF_GetBlock(fileDesc, 0, firstBlock));
    char *firstBlockData = BF_Block_GetData(firstBlock);
    int global_depth;
    memcpy(&global_depth, firstBlockData + strlen("HF") + 1, sizeof(int));
    int iterations = (int) pow(2, global_depth) / (BF_BLOCK_SIZE / 4);

    if ( iterations == 0 )iterations++;
    int hashBlocksArray[iterations];
    for ( int j = 0; j < iterations; ++j ) {
        int hashblockID;
        memcpy(&hashblockID, firstBlockData + strlen("HF") + 1 + sizeof(int) + j * sizeof(int), sizeof(int));
        hashBlocksArray[j] = hashblockID;
        printf(" hashblock id is %d\n", hashBlocksArray[j]);
    }
    BF_UnpinBlock(firstBlock);
    int uniqueBlocks;
    CALL_BF(BF_GetBlockCounter(fileDesc, &uniqueBlocks));
    BF_Block *blockPtr;
    BF_Block_Init(&blockPtr);
    char *blockPtrData;
    int counter = 0;

    for ( int i = 1; i < uniqueBlocks; ++i ) {
        if ( i == hashBlocksArray[counter] ) {
            if ( counter + 1 < iterations ) {
                counter++;
            }
            continue;
        }
        CALL_BF(BF_GetBlock(fileDesc, i, blockPtr));
        blockPtrData = BF_Block_GetData(blockPtr);
        int entries;
        memcpy(&entries, blockPtrData + sizeof(int), sizeof(int));
        Record r;
        printf("entries : %d\n", entries);
        for ( int j = 0; j < entries; ++j ) {
            memcpy(&r, blockPtrData + 2 * sizeof(int) + j * sizeof(Record), sizeof(Record));
            printf("Bucket Number: %d || ID: %d || Name: %s || Surname: %s || City: %s\n",
                   i, r.id, r.name,
                   r.surname, r.city);
        }
        CALL_BF(BF_UnpinBlock(blockPtr));

    }
    BF_Block_Destroy(&blockPtr);
    return HT_OK;
}

HT_ErrorCode CopyBlocksToArray(int *arrayOfPointers, int fileDesc, int global_depth) {


    BF_Block *secondBlock;
    BF_Block_Init(&secondBlock);

    char *firstBlockData;
    if ( global_depth <= 7 ) {
        CALL_BF(BF_GetBlock(fileDesc, 1, secondBlock));
        char *secondBlockData = BF_Block_GetData(secondBlock);
        memcpy(arrayOfPointers, secondBlockData, (int) pow(2, global_depth) * sizeof(int));
        CALL_BF(BF_UnpinBlock(secondBlock));
        return HT_OK;

    }
    BF_Block *firstBlock;
    BF_Block_Init(&firstBlock);
    CALL_BF(BF_GetBlock(fileDesc, 0, firstBlock));
    firstBlockData = BF_Block_GetData(firstBlock);

    BF_Block *HashBlockPtr;
    BF_Block_Init(&HashBlockPtr);
    char *HashBlockData;
    HashBlockData = BF_Block_GetData(HashBlockPtr);

    int hashBlocks = (int) (pow(2, global_depth) / (BF_BLOCK_SIZE / sizeof(int)));
    // we copy the already existed hashblocks back to array
    int blockNumber;
    for ( int i = 0; i < hashBlocks; ++i ) {

        memcpy(&blockNumber, firstBlockData + strlen("HF") + 1 + sizeof(int) + i * sizeof(int), sizeof(int));

        CALL_BF(BF_GetBlock(fileDesc, blockNumber, HashBlockPtr));
        HashBlockData = BF_Block_GetData(HashBlockPtr);
        for ( int j = 0; j < (BF_BLOCK_SIZE / sizeof(int)); ++j ) {
            int v;
            memcpy(&v, HashBlockData + j * sizeof(int), sizeof(int));
            arrayOfPointers[j + i * (BF_BLOCK_SIZE / sizeof(int))] = v;
        }

        CALL_BF(BF_UnpinBlock(HashBlockPtr));
    }
    CALL_BF(BF_UnpinBlock(firstBlock));
    return HT_OK;
}

HT_ErrorCode CopyArrayToBlocks(int *arrayOfPointers, int global_depth, int fileDesc) {


    BF_Block *secondBlock;
    BF_Block_Init(&secondBlock);
    char *firstBlockData;


    if ( global_depth <= 7 ) {
        int size = (int) pow(2, global_depth);
        CALL_BF(BF_GetBlock(fileDesc, 1, secondBlock));
        char *secondBlockData = BF_Block_GetData(secondBlock);
        memcpy(secondBlockData, arrayOfPointers, size * sizeof(int));
        BF_Block_SetDirty(secondBlock);
        CALL_BF(BF_UnpinBlock(secondBlock));
        BF_Block_Destroy(&secondBlock);
        return HT_OK;
    }
    BF_Block *firstBlock;
    BF_Block_Init(&firstBlock);
    CALL_BF(BF_GetBlock(fileDesc, 0, firstBlock));
    firstBlockData = BF_Block_GetData(firstBlock);
    BF_Block *newBlockToWrite;
    BF_Block_Init(&newBlockToWrite);
    int newBlocks = ((int) pow(2, global_depth) / (BF_BLOCK_SIZE / sizeof(int)));
    int blockNumber;
    char *blockData;
    for ( int i = 0; i < newBlocks; ++i ) {
        memcpy(&blockNumber, firstBlockData + strlen("HF") + 1 + sizeof(int) + i * sizeof(int), sizeof(int));
        // it allocates the suitable space in memory
        CALL_BF(BF_GetBlock(fileDesc, blockNumber, newBlockToWrite));
        blockData = BF_Block_GetData(newBlockToWrite);
        memcpy(blockData, arrayOfPointers + i * (BF_BLOCK_SIZE / sizeof(int)),
               (BF_BLOCK_SIZE / sizeof(int)) * sizeof(int));

        BF_Block_SetDirty(newBlockToWrite);
        CALL_BF(BF_UnpinBlock(newBlockToWrite));
    }
    BF_Block_Destroy(&firstBlock);
    BF_Block_Destroy(&newBlockToWrite);
    return HT_OK;
}

HT_ErrorCode HashStatistics(char *filename) {
    int fileDesc, flag = 0;

    for ( int i = 0; i < MAX_OPEN_FILES; i++ ) {
        if ( OpenHashFiles[i].FileName != NULL && strcmp(filename, OpenHashFiles[i].FileName) == 0 ) {
            fileDesc = OpenHashFiles[i].BFid;
            flag = 1;
            break;
        }
    }
    if ( flag == 0 ) {
        printf("Filename does not exist in the table.\n");
        return HT_ERROR;
    }

    BF_Block *firstBlock;
    BF_Block_Init(&firstBlock);
    CALL_BF(BF_GetBlock(fileDesc, 0, firstBlock));
    char *firstBlockData = BF_Block_GetData(firstBlock);
    int global_depth;
    memcpy(&global_depth, firstBlockData + strlen("HF") + 1, sizeof(int));
    int iterations = (int) pow(2, global_depth) / (BF_BLOCK_SIZE / 4);

    if ( iterations == 0 )iterations++;
    int hashBlocksArray[iterations];
    for ( int j = 0; j < iterations; ++j ) {
        int hashblockID;
        memcpy(&hashblockID, firstBlockData + strlen("HF") + 1 + sizeof(int) + j * sizeof(int), sizeof(int));
        hashBlocksArray[j] = hashblockID;
    }
    CALL_BF(BF_UnpinBlock(firstBlock));
    int uniqueBlocks;
    CALL_BF(BF_GetBlockCounter(fileDesc, &uniqueBlocks));
    BF_Block *blockPtr;
    BF_Block_Init(&blockPtr);
    char *blockPtrData;
    int counter = 0;

    int min = -1, max = -1, total_records = 0;
    double mid;

    for ( int i = 1; i < uniqueBlocks; ++i ) {
        if ( i == hashBlocksArray[counter] ) {
            if ( counter + 1 < iterations ) {
                counter++;
            }

            continue;
        }
        CALL_BF(BF_GetBlock(fileDesc, i, blockPtr));
        blockPtrData = BF_Block_GetData(blockPtr);
        int entries;
        memcpy(&entries, blockPtrData + sizeof(int), sizeof(int));
        total_records += entries;
        if ( min > entries || min == -1 )min = entries;
        if ( max < entries )max = entries;
        CALL_BF(BF_UnpinBlock(blockPtr));
    }
    mid = 1.0 * total_records / (uniqueBlocks - iterations - 1);

    printf("HASH STATISTICS RESULTS FOR FILE %s:\n", filename);
    printf("Hash file has %d total blocks, %d hash blocks, %d bucket blocks.\n", uniqueBlocks, iterations,
           uniqueBlocks - iterations - 1);
    printf("The minimum entries that appear in a bucket are %d.\n", min);
    printf("The maximum entries that appear in a bucket are %d.\n", max);
    printf("The mean number of entries that appear in a bucket are %.3f .\n", mid);

    return HT_OK;

}