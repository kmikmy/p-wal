#ifndef _ARIES
#define _ARIES
#include "debug.h"
#include <mutex>
#include <cstdlib>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <map>
#include <list>
#include <set>
#include <vector>

#define PAGE_N (4096*16)
#define MAX_UPDATE 100
#define MAX_WORKER_THREAD 50
#define LOG_OFFSET (1073741824) // 1GB



enum LOG_TYPE { UPDATE, COMPENSATION, PREPARE, BEGIN, END, OSfile_return};
enum OP_TYPE { NONE, INC,DEC,SUBST };
enum STATE { U,P,C }; // UNCOMMITED, PREPARED, COMMITED

typedef struct {
  uint64_t page_LSN;
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
  OP_TYPE op_type;
  int amount;
} OP;

typedef struct {
  uint32_t TransID;
  STATE State;
  uint64_t LastLSN;
  uint64_t LastOffset;
  uint64_t UndoNxtLSN;
  uint64_t UndoNxtOffset;
} Transaction;




/*
  TransTableはただのmap。スレッドセーフではないのでリカバリ時にのみ使用する。
*/
typedef std::map<uint32_t, Transaction> TransTable;


/*
  DistributedTransTableは、ワーカースレッドの数だけ動的に生成される
 */
typedef Transaction DistributedTransTable;

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
  /* ここまでで 80 bytes */
  
  char padding[432];
} Log; /* 512バイト */
#pragma pack()

typedef struct{
  uint64_t count;
  char padding[504];
} LogHeader; /**/

typedef struct {
  uint64_t mr_chkp;
  uint32_t system_xid;
  uint64_t system_last_lsn;
  bool last_exit;
} MasterRecord;

class ARIES_SYSTEM {
private:
  static int fd;

public: 
  static MasterRecord master_record;

  static void db_init(int th_num);
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
  static void set_num_group_commit(int group_param);
  static int log_write(Log *log, int th_id);
  static void log_flush();
  static void log_all_flush();
  static void log_debug(Log log);
  static void init();
  static uint64_t read_LSN();
  static uint64_t current_offset_logfile_for_id(int th_id);
};

#endif //  _ARIES
