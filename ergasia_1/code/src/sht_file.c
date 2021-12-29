#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "../include/bf.h"
#include "../include/sht_file.h"

HT_ErrorCode
SHTSplitThePointers(int *pointersArray, int destinationBlock, int global_depth, int fileDesc, BF_Block *blockToWrite);

HT_ErrorCode SHTCopyBlocksToArray(int *arrayOfPointers, int fileDesc, int global_depth, int offset);

HT_ErrorCode SHTCopyArrayToBlocks(int *arrayOfPointers, int global_depth, int fileDesc, int offset);

HT_ErrorCode SHTResize(int indexDesc, int destinationBlock, BF_Block *blockToWrite, int offset);

HT_ErrorCode SHT_Init() {
    //insert code here
    return HT_OK;
}

HT_ErrorCode
SHT_CreateSecondaryIndex(const char *sfileName, char *attrName, int attrLength, int depth, char *fileName) {
    int blockFileID;
    // CALL_BF(BF_Init(LRU));//it uses the LRU method to clear the unneeded blocks from the buffer
    CALL_BF(BF_CreateFile(sfileName));
    CALL_BF(BF_OpenFile(sfileName, &blockFileID));// it gives the data1.db file a specific ID(lets say ID=11)

    /* Block Initialization*/
    BF_Block *firstBlock, *secondBlock;
    BF_Block_Init(&firstBlock);// it allocates the suitable space in memory
    BF_Block_Init(&secondBlock);// it allocates the suitable space in memory

    /* we allocate the blocks at the end of the BF_Block file */
    CALL_BF(BF_AllocateBlock(blockFileID, firstBlock));
    CALL_BF(BF_AllocateBlock(blockFileID, secondBlock));

    char *firstBlockData = BF_Block_GetData(firstBlock);
    int two = 1;
    char *str = "SHT";
    strcpy(firstBlockData, str);

    /*
     * FIRST BLOCK STRUCTURE:
     * "SHT"->depth->hash file name length-> hash file name->attribute length (strlen("city"))
     * ->attribute name->first block evretirio location->....(all evretiria blocks locations)
     */
    memcpy(firstBlockData + strlen("SHT") + 1, &depth, sizeof(int));
    int filename_len = strlen(fileName) + 1;
    memcpy(firstBlockData + strlen("SHT") + 1 + sizeof(int), &filename_len, sizeof(int));
    memcpy(firstBlockData + strlen("SHT") + 1 + 2 * sizeof(int), fileName, strlen(fileName) + 1);
    memcpy(firstBlockData + strlen("SHT") + 1 + 2 * sizeof(int) + strlen(fileName) + 1, &attrLength, sizeof(int));
    memcpy(firstBlockData + strlen("SHT") + 1 + 3 * sizeof(int) + strlen(fileName) + 1, attrName, attrLength);
    memcpy(firstBlockData + strlen("SHT") + 1 + 3 * sizeof(int) + strlen(fileName) + 1 + attrLength, &two, sizeof(int));


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

HT_ErrorCode SHT_OpenSecondaryIndex(const char *sfileName, int *indexDesc) {

    int shID;
    BF_Block *firstBlock;
    BF_Block_Init(&firstBlock);
    CALL_BF(BF_OpenFile(sfileName, &shID));

    BF_GetBlock(shID, 0, firstBlock);
    char *firstBlockData = BF_Block_GetData(firstBlock);
    char *primaryHFname;
    int HFnamelength;
    memcpy(&HFnamelength, firstBlockData + strlen("SHT") + 1 + sizeof(int), sizeof(int));
    memcpy(primaryHFname, firstBlockData + strlen("SHT") + 1 + 2 * sizeof(int), HFnamelength);
    for ( int i = 0; i < MAX_OPEN_FILES; ++i ) {
        if ( strcmp(OpenHashFiles[i].FileName, primaryHFname) == 0 ) {
            *indexDesc = i;
            OpenSHTFiles[i].FileName = malloc(strlen(sfileName) + 1);
            strcpy(OpenSHTFiles[i].FileName, sfileName);
            OpenSHTFiles[i].BFid = shID;
            return HT_OK;
        }
    }
    printf("NO SPACE AVAILABLE FOR HASHFILE :%s\n", sfileName);
    return HT_ERROR;
}

HT_ErrorCode SHT_CloseSecondaryIndex(int indexDesc) {
    if ( indexDesc < 0 || indexDesc > MAX_OPEN_FILES ) {
        printf("INDEX OUT OF TABLE LENGTH\n");
        return HT_ERROR;
    }
    if ( OpenSHTFiles[indexDesc].FileName == NULL )printf("THERE IS NO OPEN FILE AT THIS INDEX\n");
    CALL_BF(BF_CloseFile(OpenSHTFiles[indexDesc].BFid));
    free(OpenSHTFiles[indexDesc].FileName);
    OpenSHTFiles[indexDesc].FileName = NULL;
    return HT_OK;
}

HT_ErrorCode SHT_SecondaryInsertEntry(int indexDesc, SecondaryRecord record) {
    BF_Block *firstBlock, *secondBlock;
    char *firstBlockData, *secondBlockData;
    int fileDesc = OpenSHTFiles[indexDesc].BFid;
    int numOfEntries;
    int recs = BF_BLOCK_SIZE / sizeof(SecondaryRecord);

    BF_Block_Init(&firstBlock);// it allocates the suitable space in memory
    BF_Block_Init(&secondBlock);// it allocates the suitable space in memory

    CALL_BF(BF_GetBlock(fileDesc, 0, firstBlock));
    firstBlockData = BF_Block_GetData(firstBlock);


    int global_depth;
    memcpy(&global_depth, firstBlockData + strlen("SHT") + 1, sizeof(int));


    /* we retrieve the address of the hashtable from the second block*/
    CALL_BF(BF_GetBlock(fileDesc, 1, secondBlock));
    secondBlockData = BF_Block_GetData(secondBlock);

    //hash function
    int rec_id = 0;/*to be decided ;) */;
    for ( int i = 0; i < strlen(record.index_key); i++ ) {
        rec_id += record.index_key[i];
    }
    int block_index = 0;
    for ( int i = 0; i < global_depth; i++ ) {
        block_index += (rec_id % 2) * (int) pow(2, global_depth - i - 1);
        rec_id /= 2;
    }
    /*
     * block_index is the decimal representation of the first-global_depth bits of the record id
     * lets assume that ID=7(111) and global_depth=2 so block_index=3(11)
     */

    int filename_len, attr_len;
    memcpy(&filename_len, firstBlockData + strlen("SHT") + 1 + sizeof(int), sizeof(int));
    memcpy(&attr_len, firstBlockData + strlen("SHT") + 1 + 2 * sizeof(int) + filename_len, sizeof(int));
    int offset = strlen("SHT") + 1 + 3 * sizeof(int) + filename_len + attr_len;


    int hashBlockIndex;
    memcpy(&hashBlockIndex, firstBlockData + offset +
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
    if ( numOfEntries < (BF_BLOCK_SIZE - 2 * sizeof(int)) / sizeof(SecondaryRecord)) {

        /* There is enough space so we make one more insertion*/
        numOfEntries++;
        memcpy(blockToWritePointer + sizeof(int), &numOfEntries, sizeof(int));

        //writing record to block
        /*
         * blockToWritePointer + 2 * sizeof(int) we leave 8 bytes at the beginning of the block
         * 4bytes for the LOCALDEPTH and 4 bytes for the number of entries
         */
        memcpy(blockToWritePointer + 2 * sizeof(int) + (numOfEntries - 1) * sizeof(SecondaryRecord), &record,
               sizeof(SecondaryRecord));

        BF_Block_SetDirty(blockToWrite);
        CALL_BF(BF_UnpinBlock(blockToWrite));

    } else {

        //MORE THAN 8 ENTRIES INSIDE THE BLOCK
        //blockToWrite is the block that has already 8 entries
        int local_depth;
        memcpy(&local_depth, blockToWritePointer, sizeof(int));

        if ( local_depth == global_depth ) {
            //resize and split
            CALL_OR_DIE(SHTResize(indexDesc, destinationBlock, blockToWrite, offset));
        } else if ( local_depth < global_depth ) {
            //split
            int *array = malloc(((int) pow(2, global_depth)) * sizeof(int));
            CALL_OR_DIE(SHTCopyBlocksToArray(array, fileDesc, global_depth, offset));
            CALL_OR_DIE(SHTSplitThePointers(array, destinationBlock, global_depth, fileDesc, blockToWrite));
            CALL_OR_DIE(SHTCopyArrayToBlocks(array, global_depth, fileDesc, offset));
            free(array);
        }

        firstBlockData = BF_Block_GetData(firstBlock);
        memcpy(&global_depth, firstBlockData + strlen("SHT") + 1, sizeof(int));

        /*we reinsert the records of the overflow-block*/
        SecondaryRecord *recordArray[recs + 1];
        for ( int i = 0; i < recs; ++i ) {
            recordArray[i] = malloc(sizeof(SecondaryRecord));
            memcpy(recordArray[i], blockToWritePointer + 2 * sizeof(int) + i * sizeof(SecondaryRecord),
                   sizeof(SecondaryRecord));
            // updateArray[i]->recordPointer = malloc(sizeof(Record));
        }
        //recordArray[8] = malloc(sizeof(Record));
        recordArray[recs] = &record;
        //we keep only the local depth and we erase everything else from the overflow block
        memset(blockToWritePointer + sizeof(int), 0, BF_BLOCK_SIZE - sizeof(int));
        int zero = 0;
        memcpy(blockToWritePointer + sizeof(int), &zero, sizeof(int));

        numOfEntries = 0;
        BF_Block *newBlockToWrite;
        BF_Block_Init(&newBlockToWrite);
        BF_Block_SetDirty(blockToWrite);
        BF_UnpinBlock(blockToWrite);
        for ( int i = 0; i < recs + 1; ++i ) {
            for ( int j = 0; i < strlen(recordArray[i]->index_key); i++ ) {
                rec_id += recordArray[i]->index_key[j];
            }
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

            memcpy(&hashBlockIndex, firstBlockData + offset +
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
            memcpy(newBlockToWritePointer + 2 * sizeof(int) + (numOfEntries - 1) * sizeof(SecondaryRecord),
                   recordArray[i],
                   sizeof(SecondaryRecord));

            memcpy(newBlockToWritePointer + sizeof(int), &numOfEntries, sizeof(int));

            BF_Block_SetDirty(newBlockToWrite);
            CALL_BF(BF_UnpinBlock(newBlockToWrite));
            CALL_BF(BF_UnpinBlock(hashBlock));

        }

        BF_Block_Destroy(&newBlockToWrite);
        BF_Block_Destroy(&hashBlock);

        for ( int i = 0; i < recs; ++i ) {
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

HT_ErrorCode SHTResize(int indexDesc, int destinationBlock, BF_Block *blockToWrite, int offset) {
    int fileDesc = OpenSHTFiles[indexDesc].BFid;
    char *firstBlockData, *secondBlockData, *blockData;
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
    memcpy(&global_depth, firstBlockData + strlen("SHT") + 1, sizeof(int));
    global_depth++;
    memcpy(firstBlockData + strlen("SHT") + 1, &global_depth, sizeof(int));
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

        CALL_OR_DIE(SHTSplitThePointers(PointersArray, destinationBlock, global_depth, fileDesc, blockToWrite));

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

            memcpy(&blockNumber, firstBlockData + offset + i * sizeof(int), sizeof(int));

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


        CALL_OR_DIE(SHTSplitThePointers(PointersArray, destinationBlock, global_depth, fileDesc, blockToWrite));

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
            memcpy(firstBlockData + offset + i * sizeof(int), &blockNumber, sizeof(int));
            CALL_BF(BF_UnpinBlock(newBlock));
        }
        BF_Block *newBlockToWrite;
        BF_Block_Init(&newBlockToWrite);
        for ( int i = 0; i < newBlocks; ++i ) {
            memcpy(&blockNumber, firstBlockData + offset + i * sizeof(int), sizeof(int));
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
SHTSplitThePointers(int *pointersArray, int destinationBlock, int global_depth, int fileDesc, BF_Block *blockToWrite) {


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

HT_ErrorCode SHTCopyBlocksToArray(int *arrayOfPointers, int fileDesc, int global_depth, int offset) {


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

        memcpy(&blockNumber, firstBlockData + offset + i * sizeof(int), sizeof(int));

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

HT_ErrorCode SHTCopyArrayToBlocks(int *arrayOfPointers, int global_depth, int fileDesc, int offset) {


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
        memcpy(&blockNumber, firstBlockData + offset + i * sizeof(int), sizeof(int));
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

HT_ErrorCode SHT_SecondaryUpdateEntry(int indexDesc, UpdateRecordArray *updateArray) {
    BF_Block *firstBlock;
    char *firstBlockData;
    int fileDesc = OpenSHTFiles[indexDesc].BFid;
    int numOfEntries;

    BF_Block_Init(&firstBlock);// it allocates the suitable space in memory

    CALL_BF(BF_GetBlock(fileDesc, 0, firstBlock));
    firstBlockData = BF_Block_GetData(firstBlock);

    int global_depth;
    memcpy(&global_depth, firstBlockData + strlen("SHT") + 1, sizeof(int));

    int filename_len, attr_len;
    char hashField[20];
    memcpy(&filename_len, firstBlockData + strlen("SHT") + 1 + sizeof(int), sizeof(int));
    memcpy(&attr_len, firstBlockData + strlen("SHT") + 1 + 2 * sizeof(int) + filename_len, sizeof(int));
    int offset = strlen("SHT") + 1 + 3 * sizeof(int) + filename_len;
    memcpy(hashField, firstBlockData + offset, attr_len);


    for ( int j = 0; j <= 7; j++ ) {
        //hash function
        if ( updateArray->array[j].new_tupleid == updateArray->array[j].old_tupleid )continue;

        char hashKey[20];
        if ( strcmp(hashField, "city") == 0 )
            strcpy(hashKey, updateArray->array[j].recordPointer->city);
        else if ( strcmp(hashField, "surname") == 0 )
            strcpy(hashKey, updateArray->array[j].recordPointer->surname);
        else
            return HT_ERROR;

        int rec_id = 0;/*to be decided ;) */;
        for ( int i = 0; i < strlen(hashKey); i++ ) {
            rec_id += hashKey[i];
        }
        int block_index = 0;
        for ( int i = 0; i < global_depth; i++ ) {
            block_index += (rec_id % 2) * (int) pow(2, global_depth - i - 1);
            rec_id /= 2;
        }
        /*
         * block_index is the decimal representation of the first-global_depth bits of the record id
         * lets assume that ID=7(111) and global_depth=2 so block_index=3(11)
         */

        int filename_len, attr_len;
        memcpy(&filename_len, firstBlockData + strlen("SHT") + 1 + sizeof(int), sizeof(int));
        memcpy(&attr_len, firstBlockData + strlen("SHT") + 1 + 2 * sizeof(int) + filename_len, sizeof(int));
        int offset = strlen("SHT") + 1 + 3 * sizeof(int) + filename_len + attr_len;


        int hashBlockIndex;
        memcpy(&hashBlockIndex, firstBlockData + offset +
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

        memcpy(&destinationBlock, hashBlockData + (block_index % (BF_BLOCK_SIZE / sizeof(int))) * sizeof(int),
               sizeof(int));

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
        for ( int i = 0; i < numOfEntries; ++i ) {
            SecondaryRecord srecord;
            memcpy(&srecord, blockToWritePointer + 2 * sizeof(int) + numOfEntries * sizeof(SecondaryRecord),
                   sizeof(SecondaryRecord));
            if ( srecord.tupleId == updateArray->array[j].old_tupleid ) {
                if ( !strcmp(srecord.index_key, hashKey)) {
                    srecord.tupleId = updateArray->array[j].new_tupleid;
                    memcpy(blockToWritePointer + 2 * sizeof(int) + numOfEntries * sizeof(SecondaryRecord), &srecord,
                           sizeof(SecondaryRecord));
                    break;
                }
            }
        }
        BF_Block_SetDirty(blockToWrite);
        BF_UnpinBlock(blockToWrite);
        BF_Block_Destroy(&blockToWrite);
        BF_UnpinBlock(hashBlock);
    }
    BF_UnpinBlock(firstBlock);
    return HT_OK;
}

HT_ErrorCode SHT_PrintAllEntries(int sindexDesc, char *index_key) {
    //insert code here
    return HT_OK;
}

HT_ErrorCode SHT_HashStatistics(char *filename) {
    //insert code here
    return HT_OK;
}

HT_ErrorCode SHT_InnerJoin(int sindexDesc1, int sindexDesc2, char *index_key) {
    //insert code here
    return HT_OK;
}