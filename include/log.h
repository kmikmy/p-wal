#ifndef _log
#define _log

#include <cstdlib>
#include <cstdint>

#define LOG_OFFSET (1073741824) // 1GB

enum LOG_TYPE { UPDATE, COMPENSATION, PREPARE, BEGIN, END, OSfile_return, INSERT};
enum TABLE_TYPE { SIMPLE, WAREHOUSE, DISTRICT, CUSTOMER, HISTORY, ORDER, ORDERLINE, NEWORDER, ITEM, STOCK };
enum OP_TYPE { NONE, INC,DEC,SUBST,READ };


typedef struct {
  OP_TYPE op_type;
  int amount;
} OP;

#pragma pack(1)

class LogRecordHeader{
 public:
  size_t totalLength, fieldLength;
  uint64_t LSN, offset, prevLSN, prevOffset, undoNxtLSN, undoNxtOffset;
  uint64_t transID, pageID; // TransactionID & PageID
  int fileID; // File ID
  LogType type; // Tyep of log
  char tableName[MAX_TABLE_NAME]; // Table name
  size_t fieldCnt; // The number of field in the Log
};

typedef struct{
  size_t totalLength, fieldLength;
  uint64_t LSN, offset, prevLSN, prevOffset, undoNxtLSN, undoNxtOffset;
  uint64_t transID, pageID; // TransactionID & PageID
  int fileID; // File ID
  LogType type; // Tyep of log
  char tableName[MAX_TABLE_NAME]; // Table name
  size_t fieldCnt; // The number of field in the Log

  // この後にフィールドログがN個続く
  // FieldLog fLog[fieldCnt];
} Log;

typedef struct {
  size_t fieldOffset, fieldLength; // タプルの何バイト目から何バイトの長さの情報

  // この後にフィールドの更新前、更新後の値が続く
  //  char before[fieldLength], after[fieldLength];
} FieldLog;


/* Logger::log_write()に渡す型 */
typedef struct {
 public:
  size_t fieldOffset, fieldLength;  
  char *before, *after;
  FieldLogList *nxt;
} FieldLogList;

typedef struct{
  uint64_t chunkNum; // The number of Chunk Log.
  off_t fileSize; // File size
  char padding[496]; 
} LogSegmentHeader; /* 512 Bytes */


class Logger
{
 private:

 public:
  static const char* logpath;
  static int log_write(Log *log, FieldLogList *fieldLogList, int th_id);
  static void log_flush(int th_id);
  static void log_all_flush();
  static void log_debug(Log log);
  static void init();
  static uint64_t read_LSN();
  static uint64_t current_offset_logfile_for_id(int th_id);
};


/* typedef struct { */
/*   uint64_t LSN; */
/*   uint64_t Offset; */
/*   uint64_t PrevLSN; */
/*   uint64_t PrevOffset; */
/*   uint64_t UndoNxtLSN; */
/*   uint64_t UndoNxtOffset; */
/*   /\* ここまでで 48 bytes  *\/ */

/*   int file_id; */
/*   uint32_t TransID; */
/*   LOG_TYPE Type; */
/*   uint32_t PageID; */
/*   int before; */
/*   int after; */
/*   OP op; // 8 bytes */
/*   TABLE_TYPE tid;  */
/*   /\* ここまでで 84 bytes *\/ */
/*   char padding[428]; */
/*   /\* ここまでで 512 bytes *\/ */
/*   //  char padding2[512]; */
/*   //  char padding3[1024]; */
/* } Log; /\* 1024バイト *\/ */
#pragma pack()

#endif
