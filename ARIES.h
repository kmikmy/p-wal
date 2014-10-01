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

#define PAGE_N (4096*16)
#define MAX_UPDATE 100

enum LOG_TYPE { UPDATE, COMPENSATION, PREPARE, BEGIN, END, OSfile_return};
enum OP_TYPE { NONE, INC,DEC,SUBST };
enum STATE { U,P,C }; // UNCOMMITED, PREPARED, COMMITED

typedef struct {
  uint32_t page_LSN;
  int pageID;
  int value;  
} Page;

typedef struct PageBufferEntry{
  pthread_rwlock_t lock; // protect BufEntry

  uint32_t page_id;
  bool fixed_flag;
  bool modified_flag;

  Page page;
} PageBufferEntry;

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
   TransQueue:
   
   vectorでqueueを実装.
   std::queue.front()は参照を返すので、popで削除すると問題があるため.
*/
typedef class {
 private:
  unsigned max_size;
  pthread_mutex_t mutex;
  std::vector<Transaction> real_vector;

 public:
  int setsize(unsigned n)
  {
    if(n > 256)
      return -1;

    max_size = n;
    return 0;
  }

  void init()
  {
    pthread_mutex_init(&mutex,NULL);
  }
  void lock()
  {
    pthread_mutex_lock(&mutex);
  }
  void unlock()
  {
    pthread_mutex_unlock(&mutex);
  }

  bool empty(){
    return real_vector.empty();
  }
  size_t size(){
    return real_vector.size();
  }
  bool full(){
    return size() >= max_size;
  }
  Transaction front(){
    return real_vector.front();
  }
  void pop(){
    real_vector.erase(real_vector.begin());
  }
  void push(const Transaction &trans){
    if(size() >= max_size)
      return;

    real_vector.push_back(trans);
  }

} TransQueue;


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
  OP op;
} Log;


typedef struct{
  uint32_t count;
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
  static int fd2;

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
  static int log_write(Log *log);
  static void log_flush();
  static void log_debug(Log log);
};

#endif //  _ARIES
