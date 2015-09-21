#ifndef _log
#define _log

#include <cstdlib>
#include <cstdint>
#include "table.h"

#define LOG_OFFSET (1073741824) // 1GB

enum kLogType { UPDATE, COMPENSATION, PREPARE, BEGIN, END, OSfile_return, INSERT};
enum kTableType { SIMPLE, WAREHOUSE, DISTRICT, CUSTOMER, HISTORY, ORDER, ORDERLINE, NEWORDER, ITEM, STOCK };
enum OP_TYPE { NONE, INC,DEC,SUBST,READ };

extern const int kMaxTableNameLen;

typedef struct {
  OP_TYPE op_type;
  int amount;
} OP;

#pragma pack(1)

typedef struct{
  uint64_t chunk_num;
  uint64_t log_num;
  off_t segment_size; // File size
  char padding[488];
} LogSegmentHeader; /* 512 Bytes */

typedef struct{
  uint64_t chunk_size;
  uint64_t log_record_num;;
} ChunkLogHeader; /* 512 Bytes */

typedef struct{
  size_t total_length, total_field_length;
  uint64_t lsn, offset, prev_lsn, prev_offset, undo_nxt_lsn, undo_nxt_offset;
  uint64_t trans_id, page_id;
  int file_id;
  kLogType type; // Tyep of log
  kTableType table_id;
  char table_name[kMaxTableNameLen];
  size_t field_num; // The number of field in the Log

  // この後に(FieldLog, before value, after value)がN個続く
  // {
  // FieldLog fLog
  // char *before, *aftegr
  // } * N
} Log;

typedef Log LogRecordHeader;

typedef struct {
  size_t fieldOffset, fieldLength; // タプルの何バイト目から何バイトの長さの情報
  // この後にフィールドの更新前、更新後の値が続く
  //  char before[fieldLength], after[fieldLength];
} FieldLogHeader;

/* Logger::log_write()に渡す型 */
typedef struct _FieldLogList {
 public:
  size_t fieldOffset, fieldLength;
  char *before, *after;
  _FieldLogList *nxt;
} FieldLogList;

#pragma pack()

class Logger
{
 private:

 public:
  static uint32_t num_group_commit;

  static const char* logpath;
  static void set_num_group_commit(int group_param);
  static int log_write(Log *log, FieldLogList *field_log_list, int th_id);
  static void log_flush(int th_id);
  static void log_all_flush();
  static void log_debug(Log log);
  static void init();
  static uint64_t read_LSN();
  static uint64_t current_offset_logfile_for_id(int th_id);
};


#endif
