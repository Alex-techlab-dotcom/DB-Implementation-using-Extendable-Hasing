typedef struct HashIndex{
    int id;
    BF_Block* blockPointer;
}HashIndex;

typedef struct HashTable{
    int depth;
    HashIndex *h_array;
}HashTable;