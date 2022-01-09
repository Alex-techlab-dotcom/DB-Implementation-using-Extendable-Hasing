#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "../include/sht_file.h"
#include "../include/hash_file.h"
#include "../include/bf.h"
#define GLOBAL_DEPT 2 // you can change it if you want
#define FILE_NAME "data.db"
#define SHT_FILE_NAME "shtdata.db"
#define FILE_NAME_2 "data2.db"
#define SHT_FILE_NAME_2 "shtdata2.db"
#define RECORD_FILE "examples/records.txt"

#define CALL_OR_DIE(call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }



    int main() {

      //create ht file and sht file
        CALL_OR_DIE(HT_Init());
        int indexDesc;
        int SHTindex;
        CALL_OR_DIE(HT_CreateIndex(FILE_NAME, GLOBAL_DEPT));
        CALL_OR_DIE(SHT_CreateSecondaryIndex(SHT_FILE_NAME,"surname",20,GLOBAL_DEPT,FILE_NAME));
        CALL_OR_DIE(HT_OpenIndex(FILE_NAME, &indexDesc));
        CALL_OR_DIE(SHT_OpenSecondaryIndex(SHT_FILE_NAME,&SHTindex));
        
        Record record;
        int id;
	      char * name;
	      char * surname;
	      char * city;
        FILE *file;
        file = fopen(RECORD_FILE, "r");
        char* buffer = malloc(1024);
        int counter=0;
        UpdateRecordArray array;
        int tupleId=0;
        while(fgets(buffer, 1024, file)!=NULL && ++counter<100){
          id = atoi(strtok(buffer,"\t"));
          name = strtok(NULL,"\t");
          surname = strtok(NULL,"\t");
          city = strtok(NULL,"\t");
          record.id=id;
          memcpy(record.name, name, strlen(name) + 1);
          memcpy(record.surname, surname, strlen(surname) + 1);
          memcpy(record.city, city, strlen(city) + 1);

          CALL_OR_DIE(HT_InsertEntry(indexDesc, record,&tupleId,&array));
            if(array.hasResults){
                CALL_OR_DIE(SHT_SecondaryUpdateEntry(SHTindex,&array));
            }
            SecondaryRecord  sr;
            sr.tupleId=tupleId;
            strcpy(sr.index_key,record.surname);
            CALL_OR_DIE(SHT_SecondaryInsertEntry(SHTindex,sr));
        }

        id=50;
        printf("The entry with id 50 is:\n");
        HT_PrintAllEntries(indexDesc, &id); //print the entry with id = 50 (for the whole table to be printed call HT_PrintAllEntries(indexDesc, NULL))

        printf("\nThe entries with surname MILLER are:\n");
        SHT_PrintAllEntries(SHTindex, "MILLER"); //print all entries with surname = SMITH

        //insert a new record to primary and secondary hash table
        if(fgets(buffer, 1024, file)!=NULL){
          id = atoi(strtok(buffer,"\t"));
          name = strtok(NULL,"\t");
          surname = strtok(NULL,"\t");
          city = strtok(NULL,"\t");
          record.id=id;
          memcpy(record.name, name, strlen(name) + 1);
          memcpy(record.surname, surname, strlen(surname) + 1);
          memcpy(record.city, city, strlen(city) + 1);

          CALL_OR_DIE(HT_InsertEntry(indexDesc, record,&tupleId,&array));
            if(array.hasResults){
                CALL_OR_DIE(SHT_SecondaryUpdateEntry(SHTindex,&array));
            }
            SecondaryRecord  sr;
            sr.tupleId=tupleId;
            strcpy(sr.index_key,record.surname);
            CALL_OR_DIE(SHT_SecondaryInsertEntry(SHTindex,sr));
        }

        printf("\nThe last inserted entry is:\n");
        HT_PrintAllEntries(indexDesc, &id); //print the new entry inserted

        printf("\nThe entries with the same surname is the last inserted are:\n");
        SHT_PrintAllEntries(SHTindex, surname); //print all entries with surname = new surname inserted

        CALL_OR_DIE(HashStatistics(FILE_NAME));
        printf("\n\n");
        CALL_OR_DIE(SHT_HashStatistics(SHT_FILE_NAME));


        //create 2nd set of hash tables to demonstrate inner join function
        int indexDesc2;
        int SHTindex2;
        CALL_OR_DIE(HT_CreateIndex(FILE_NAME_2, GLOBAL_DEPT));
        CALL_OR_DIE(SHT_CreateSecondaryIndex(SHT_FILE_NAME_2,"surname",20,GLOBAL_DEPT,FILE_NAME_2));
        CALL_OR_DIE(HT_OpenIndex(FILE_NAME_2, &indexDesc2));
        CALL_OR_DIE(SHT_OpenSecondaryIndex(SHT_FILE_NAME_2,&SHTindex2));

        //insert records to new hash tables
        tupleId=0;
        counter=0;
        while(fgets(buffer, 1024, file)!=NULL && ++counter<200){
          id = atoi(strtok(buffer,"\t"));
          name = strtok(NULL,"\t");
          surname = strtok(NULL,"\t");
          city = strtok(NULL,"\t");
          record.id=id;
          memcpy(record.name, name, strlen(name) + 1);
          memcpy(record.surname, surname, strlen(surname) + 1);
          memcpy(record.city, city, strlen(city) + 1);

          CALL_OR_DIE(HT_InsertEntry(indexDesc2, record,&tupleId,&array));
            if(array.hasResults){
                CALL_OR_DIE(SHT_SecondaryUpdateEntry(SHTindex2,&array));
            }
            SecondaryRecord  sr;
            sr.tupleId=tupleId;
            strcpy(sr.index_key,record.surname);
            CALL_OR_DIE(SHT_SecondaryInsertEntry(SHTindex2,sr));
        }

        SHT_PRINTALL(SHTindex2);

        //join the 2 hash tables on surname=HALL (call the function like SHT_InnerJoin(SHTindex, SHTindex2, NULL) for all joins to be displayed)
        printf("\nINNER JOIN SHT1, SHT2 WHERE Index_key = MILLER :\n");
        CALL_OR_DIE(SHT_InnerJoin(SHTindex, SHTindex2, "MILLER" ));

        CALL_OR_DIE(SHT_CloseSecondaryIndex(SHTindex));
        CALL_OR_DIE(HT_CloseFile(indexDesc));

        CALL_OR_DIE(SHT_CloseSecondaryIndex(SHTindex2));
        CALL_OR_DIE(HT_CloseFile(indexDesc2));

        free(buffer);

        BF_Close();
    }
