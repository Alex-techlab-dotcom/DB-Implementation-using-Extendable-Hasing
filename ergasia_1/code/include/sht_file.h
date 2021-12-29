#ifndef HASH_FILE_H
#define HASH_FILE_H

#include "hash_file.h"

#define MAX_OPEN_SHT_FILES 20

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
char index_key[20];
int tupleId;  /*Ακέραιος που προσδιορίζει το block και τη θέση μέσα στο block στην οποία     έγινε η εισαγωγή της εγγραφής στο πρωτεύον ευρετήριο.*/
}SecondaryRecord;

typedef struct UpdateRecordNode{
    Record* recordPointer;
    int new_tupleid;
    int old_tupleid;
}UpdateRecordNode;

typedef struct UpdateRecordArray{
    UpdateRecordNode array[8];
    char hasResults;
}
        UpdateRecordArray;

typedef struct SHFcell{
    int BFid;
    char* FileName;
}SHFcell;

SHFcell OpenSHTFiles[MAX_OPEN_SHT_FILES];

HT_ErrorCode SHT_Init();

HT_ErrorCode SHT_CreateSecondaryIndex(
const char *sfileName, /* όνομα αρχείου */
char *attrName, /* όνομα πεδίου-κλειδιού */
int attrLength, /* μήκος πεδίου-κλειδιού */
int depth,/* το ολικό βάθος ευρετηρίου επεκτατού κατακερματισμού */
char *fileName /* όνομα αρχείου πρωτεύοντος ευρετηρίου*/);

HT_ErrorCode SHT_OpenSecondaryIndex(
const char *sfileName, /* όνομα αρχείου */
int *indexDesc /* θέση στον πίνακα με τα ανοιχτά αρχεία που επιστρέφεται */ );

HT_ErrorCode SHT_CloseSecondaryIndex(int indexDesc /* θέση στον πίνακα με τα ανοιχτά αρχεία */ );

HT_ErrorCode SHT_SecondaryInsertEntry (
int indexDesc, /* θέση στον πίνακα με τα ανοιχτά αρχεία */
SecondaryRecord record /* δομή που προσδιορίζει την εγγραφή */ );

HT_ErrorCode SHT_SecondaryUpdateEntry (
int indexDesc, /* θέση στον πίνακα με τα ανοιχτά αρχεία */
UpdateRecordArray *updateArray /* δομή που προσδιορίζει την παλιά εγγραφή */);

HT_ErrorCode SHT_PrintAllEntries(
int sindexDesc, /* θέση στον πίνακα με τα ανοιχτά αρχεία  του αρχείου δευτερεύοντος ευρετηρίου */
char *index_key /* τιμή του πεδίου-κλειδιού προς αναζήτηση */);

HT_ErrorCode SHT_HashStatistics(char *filename /* όνομα του αρχείου που ενδιαφέρει */);

HT_ErrorCode SHT_InnerJoin(
int sindexDesc1, /* θέση στον πίνακα με τα ανοιχτά αρχεία  του αρχείου δευτερεύοντος ευρετηρίου για το πρώτο αρχείο εισόδου */
int sindexDesc2, /* θέση στον πίνακα με τα ανοιχτά αρχεία του αρχείου δευτερεύοντος ευρετηρίου για το δεύτερο αρχείο εισόδου */
char *index_key  /* το κλειδι πανω στο οποιο θα γινει το join. Αν  NULL τοτε επιστρέφεί όλες τις πλειάδες*/);


#endif // HASH_FILE_H
