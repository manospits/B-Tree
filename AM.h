#ifndef AM_H_
#define AM_H_

/* Error codes */

extern int AM_errno;


#define AME_OK 0
#define AME_EOF -1
//TODO: Add your error codes
#define AME_FILE_CR_FAIL -2
#define AME_FILE_OP_FAIL -3
#define AME_DESTR_FAIL -4
#define AME_CLOSE_FAIL -5
#define AME_INSERT_FAIL -6
#define AME_NO_SPACE -21
#define AME_SCAN_OPEN -22
#define AME_SCAN_CLOSED -23
#define AME_FILE_READ_FAILED -24
#define AME_FILE_NOT_OPEN -25
#define AME_WRITE_BLOCK_FAILED -26
#define AME_ALLOCATE_BLOCK_FAILED -27
#define AME_GET_BLOCK_C_FAILED -28
#define AME_CREATE_FILE_FAILED -29
#define AME_OPEN_FILE_FAILED -30
#define AME_CLOSE_FILE_FAILED -31
#define AME_MALLOC_FAILED -32

#define EQUAL 1
#define NOT_EQUAL 2
#define LESS_THAN 3
#define GREATER_THAN 4
#define LESS_THAN_OR_EQUAL 5
#define GREATER_THAN_OR_EQUAL 6
#define AMFILE "AM_FILE_HEADER"

#define MAXOPENFILES 20
#define MAXSCANS 20

void AM_Init( void );

int AM_CreateIndex(
  char *fileName, /* όνομα αρχείου */
  char attrType1, /* τύπος πρώτου πεδίου: 'c' (συμβολοσειρά), 'i' (ακέραιος), 'f' (πραγματικός) */
  int attrLength1, /* μήκος πρώτου πεδίου: 4 γιά 'i' ή 'f', 1-255 γιά 'c' */
  char attrType2, /* τύπος πρώτου πεδίου: 'c' (συμβολοσειρά), 'i' (ακέραιος), 'f' (πραγματικός) */
  int attrLength2 /* μήκος δεύτερου πεδίου: 4 γιά 'i' ή 'f', 1-255 γιά 'c' */
);


int AM_DestroyIndex(
  char *fileName /* όνομα αρχείου */
);


int AM_OpenIndex (
  char *fileName /* όνομα αρχείου */
);


int AM_CloseIndex (
  int fileDesc /* αριθμός που αντιστοιχεί στο ανοιχτό αρχείο */
);


int AM_InsertEntry(
  int fileDesc, /* αριθμός που αντιστοιχεί στο ανοιχτό αρχείο */
  void *value1, /* τιμή του πεδίου-κλειδιού προς εισαγωγή */
  void *value2 /* τιμή του δεύτερου πεδίου της εγγραφής προς εισαγωγή */
);


int AM_OpenIndexScan(
  int fileDesc, /* αριθμός που αντιστοιχεί στο ανοιχτό αρχείο */
  int op, /* τελεστής σύγκρισης */
  void *value /* τιμή του πεδίου-κλειδιού προς σύγκριση */
);


void *AM_FindNextEntry(
  int scanDesc /* αριθμός που αντιστοιχεί στην ανοιχτή σάρωση */
);


int AM_CloseIndexScan(
  int scanDesc /* αριθμός που αντιστοιχεί στην ανοιχτή σάρωση */
);


void AM_PrintError(
  char *errString /* κείμενο για εκτύπωση */
);


#endif /* AM_H_ */
