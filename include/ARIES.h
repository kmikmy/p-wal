#ifndef _ARIES
#define _ARIES
#include "debug.h"
#include "log.h"
#include "query.h"
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
#endif //  _ARIES
