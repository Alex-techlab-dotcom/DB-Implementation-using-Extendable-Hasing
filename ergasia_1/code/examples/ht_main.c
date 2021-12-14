#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "../include/bf.h"
#include "../include/hash_file.h"

#define RECORDS_NUM 1000 // you can change it if you want
#define GLOBAL_DEPT 3 // you can change it if you want
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


int main() {

    //int array[]={26,14,16,12,10,21,17};

    CALL_OR_DIE(HT_Init());
    int indexDesc;
    CALL_OR_DIE(HT_CreateIndex(FILE_NAME, GLOBAL_DEPT));// creates a HashFile:"data.db" and gives GlobalDept=2 for the hashmap
    CALL_OR_DIE(HT_OpenIndex(FILE_NAME, &indexDesc)); //it opens "data.db" and it returns its index( lets say it is index 10 ) to the HashFiles Array[20]

    Record record;
    srand(12569874);
    int r;
    printf("Insert Entries\n");
    // Insertion of 1000 entries!
    int numOfEntries=10000;
    for (int id = 0; id < numOfEntries; ++id) {
        // create a record
        record.id = id;
       // printf("record.id=%d\n",record.id);
        r = rand() % 12;
        memcpy(record.name, names[r], strlen(names[r]) + 1);
        r = rand() % 12;
        memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
        r = rand() % 10;
        memcpy(record.city, cities[r], strlen(cities[r]) + 1);

        CALL_OR_DIE(HT_InsertEntry(indexDesc, record));// Array[indexDesc=10].inserts(record)
      //  CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));


    }

    CALL_OR_DIE(HashStatistics(FILE_NAME));

    CALL_OR_DIE(HT_CloseFile(indexDesc));
    BF_Close();
}
