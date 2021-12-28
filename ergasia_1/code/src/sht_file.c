#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/bf.h"
#include "../include/sht_file.h"

#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

#ifndef HASH_FILE_H
#define HASH_FILE_H


typedef enum HT_ErrorCode {
  HT_OK,
  HT_ERROR
} HT_ErrorCode;

typedef struct Record {
	int id;
	char name[15];
	char surname[20];
	char city[20];
} Record;

typedef struct{
char index-key[20];
int tupleId;  /*Ακέραιος που προσδιορίζει το block και τη θέση μέσα στο block στην οποία     έγινε η εισαγωγή της εγγραφής στο πρωτεύον ευρετήριο.*/ 
}SecondaryRecord;



HT_ErrorCode SHT_Init() {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_CreateSecondaryIndex(const char *sfileName, char *attrName, int attrLength, int depth,char *fileName ) {
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
     * "SHF"->depth->hash file name length-> hash file name->attribute length (strlen("city"))
     * ->attribute name->first block evretirio location->....(all evretiria blocks locations)
     */
    memcpy(firstBlockData + strlen("SHT") + 1, &depth, sizeof(int));
    memcpy(firstBlockData + strlen("SHT") + 1 + sizeof(int),strlen(fileName)+1, sizeof(int));
    memcpy(firstBlockData + strlen("SHT") + 1 + 2*sizeof(int), fileName, strlen(fileName)+1);
    memcpy(firstBlockData + strlen("SHT") + 1 + 2*sizeof(int)+strlen(fileName)+1, attrLength, sizeof(int));
    memcpy(firstBlockData + strlen("SHT") + 1 + 3*sizeof(int)+strlen(fileName)+1, attrName, attrLength);
    memcpy(firstBlockData + strlen("SHT") + 1 + 3*sizeof(int)+strlen(fileName)+1+attrLength, &two, sizeof(int));


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

HT_ErrorCode SHT_OpenSecondaryIndex(const char *sfileName, int *indexDesc  ) {

    int shID;
    BF_Block* firstBlock;
    BF_Block_Init(&firstBlock);
    CALL_BF(BF_OpenFile(sfileName, &shID));

    BF_GetBlock(shID,0,firstBlock);
    char *firstBlockData = BF_Block_GetData(firstBlock);
    char* primaryHFname;
    int HFnamelength;
    memcpy(&HFnamelength,firstBlockData + strlen("SHT") + 1 + sizeof(int), sizeof(int));
    memcpy(primaryHFname,firstBlockData + strlen("SHT") + 1 + 2*sizeof(int),HFnamelength);
    for ( int i = 0; i < MAX_OPEN_FILES; ++i ) {
        if ( strcmp(OpenHashFiles[i].FileName, primaryHFname)==0 ) {
            *indexDesc = i;
            OpenSHTFiles[i].Filename = malloc(strlen(sfileName)+1);
            strcpy(OpenSHTFiles[i].FileName, sfileName);
            OpenSHTFiles[i].BFid=shID;
            return HT_OK;
        }
    }
    printf("NO SPACE AVAILABLE FOR HASHFILE :%s\n", fileName);
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

HT_ErrorCode SHT_SecondaryInsertEntry (int indexDesc,SecondaryRecord record  ) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_SecondaryUpdateEntry (int indexDesc, UpdateRecordArray *updateArray ) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_PrintAllEntries(int sindexDesc, char *index-key ) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_HashStatistics(char *filename ) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_InnerJoin(int sindexDesc1, int sindexDesc2,  char *index-key ) {
  //insert code here
  return HT_OK;
}


#endif // HASH_FILE_H