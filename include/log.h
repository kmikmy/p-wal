#ifndef _ARIES
#define _ARIES

enum LOG_TYPE { UPDATE, COMPENSATION, PREPARE, BEGIN, END, OSfile_return, INSERT};
enum TABLE_TYPE { SIMPLE, WAREHOUSE, DISTRICT, CUSTOMER, HISTORY, ORDER, ORDERLINE, NEWORDER, ITEM, STOCK };
enum OP_TYPE { NONE, INC,DEC,SUBST,READ };


typedef struct {
  OP_TYPE op_type;
  int amount;
} OP;

#pragma pack(1)
typedef struct {
  uint64_t LSN;
  uint64_t Offset;
  uint64_t PrevLSN;
  uint64_t PrevOffset;
  uint64_t UndoNxtLSN;
  uint64_t UndoNxtOffset;
  /* ここまでで 48 bytes  */

  int file_id;
  uint32_t TransID;
  LOG_TYPE Type;
  uint32_t PageID;
  int before;
  int after;
  OP op; // 8 bytes
  TABLE_TYPE tid; 
  /* ここまでで 84 bytes */
  char padding[428];
  /* ここまでで 512 bytes */
  //  char padding2[512];
  //  char padding3[1024];
} Log; /* 1024バイト */
#pragma pack()

#endif
