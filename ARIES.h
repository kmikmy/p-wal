#ifndef _ARIES
#define _ARIES
#include <mutex>
#include <cstdlib>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <map>
#include <cstring>
#include <vector>
#include <set>

#define PAGE_N (4096*16)
#define MAX_UPDATE 100
#define MAX_WORKER_THREAD 50
#define LOG_OFFSET (1073741824) // 1GB



enum LOG_TYPE { UPDATE, COMPENSATION, PREPARE, BEGIN, END, OSfile_return};
enum OP_TYPE { NONE, INC,DEC,SUBST };
enum STATE { U,P,C }; // UNCOMMITED, PREPARED, COMMITED

typedef struct {
  uint32_t page_LSN;
  int pageID;
  int value;  
} Page;

typedef struct {
  pthread_rwlock_t lock; // protect BufEntry

  uint32_t page_id;
  bool readed_flag;
  bool modified_flag;
  uint32_t fixed_count;

  Page page;
} BufferControlBlock;


typedef struct {
  uint32_t pageID;
  uint32_t recLSN;
} DP_Entry;

typedef struct {
  uint32_t body_length;
} DP_Table_Head;

typedef struct {
  DP_Table_Head dp_h;
  DP_Entry* dp_entry;  
} DP_Table;

typedef struct {
  OP_TYPE op_type;
  int amount;
} OP;

typedef struct {
  uint32_t TransID;
  STATE State;
  uint32_t LastLSN;
  uint32_t UndoNxtLSN;
} Transaction;




/*
  TransTableはただのmap
*/
typedef std::map<uint32_t, Transaction> TransTable;

typedef struct {
  uint32_t LSN;
  uint32_t TransID;
  LOG_TYPE Type;
  uint32_t PrevLSN;
  uint32_t UndoNxtLSN;
  uint32_t PageID;
  int before;
  int after;
  OP op; // 8バイト
  char padding[24];
} Log; // 64バイト


typedef struct{
  uint64_t count;
} LogHeader;

typedef struct {
  uint32_t mr_chkp;
  uint32_t system_xid;
  uint32_t system_last_lsn;
  bool last_exit;
} MasterRecord;

class ARIES_SYSTEM {
private:
  static int fd;

public: 
  static MasterRecord master_record;

  static void db_init();
  static uint32_t xid_inc();
  static uint32_t xid_read();

  static int normal_exit();
  static int abnormal_exit();

  static void transtable_debug();
  
};

class Logger
{
 private:

 public:
  static const char* logpath;
  static int log_write(Log *log, int th_id);
  static void log_flush();
  static void log_all_flush();
  static void log_debug(Log log);
  static void init();
};

#endif //  _ARIES
