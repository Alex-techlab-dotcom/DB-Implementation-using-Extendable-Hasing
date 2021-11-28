#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf.h"
#include "exhashtable.h"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

int main() {

}


