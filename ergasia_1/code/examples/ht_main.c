#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"

#define RECORDS_NUM 1000 // you can change it if you want
#define GLOBAL_DEPT 2 // you can change it if you want
#define FILE_NAME "data.db"

const char* names[] = {
        "Yannis",
        "Christofos",
        "Sofia",
        "Marianna",
        "Vagelis",
        "Maria",
        "Iosif",
        "Dionisis",
        "Konstantina",
        "Theofilos",
        "Giorgos",
        "Dimitris"
};

const char* surnames[] = {
        "Ioannidis",
        "Svingos",
        "Karvounari",
        "Rezkalla",
        "Nikolopoulos",
        "Berreta",
        "Koronis",
        "Gaitanis",
        "Oikonomou",
        "Mailis",
        "Michas",
        "Halatsis"
};

const char* cities[] = {
        "Athens",
        "San Francisco",
        "Los Angeles",
        "Amsterdam",
        "London",
        "New York",
        "Tokyo",
        "Hong Kong",
        "Munich",
        "Miami"
};

#define CALL_OR_DIE(call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

int main() {
    BF_Init(LRU);

    CALL_OR_DIE(HT_Init());

    int indexDesc;
    CALL_OR_DIE(HT_CreateIndex(FILE_NAME, GLOBAL_DEPT));// creates a HashFile:"data.db" and gives GlobalDept=2 for the hashmap
    CALL_OR_DIE(HT_OpenIndex(FILE_NAME, &indexDesc)); //it opens "data.db" and it returns its index( lets say it is index 10 ) to the HashFiles Array[20]

    Record record;
    srand(12569874);
    int r;
    printf("Insert Entries\n");
    // Insertion of 1000 entries!
    for (int id = 0; id < RECORDS_NUM; ++id) {
        // create a record
        record.id = id;
        r = rand() % 12;
        memcpy(record.name, names[r], strlen(names[r]) + 1);
        r = rand() % 12;
        memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
        r = rand() % 10;
        memcpy(record.city, cities[r], strlen(cities[r]) + 1);

        CALL_OR_DIE(HT_InsertEntry(indexDesc, record));// Array[indexDesc=10].inserts(record)
    }

    printf("RUN PrintAllEntries\n");
    int id = rand() % RECORDS_NUM;
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));
    //CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));


    CALL_OR_DIE(HT_CloseFile(indexDesc));
    BF_Close();
}
