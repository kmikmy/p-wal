#include "ARIES.h"
#include <iostream>

using namespace std;

#define CAS(addr, oldval, newval) __sync_bool_compare_and_swap(addr, oldval, newval)
#define CAS64(addr, oldval, newval) __sync_bool_compare_and_swap((long *)addr, *(long *)&oldval, *(long *)&newval
#define CAS128(addr, oldval, newval) __sync_bool_compare_and_swap((__int128_t *)(addr), *(__int128_t *)&(oldval), *(__int128_t *)&(newval))
// #define DEBUG

#define NUM_GROUP_COMMIT 10
#define NUM_MAX_CORE 7
 // 1GB
#define LOG_OFFSET (1073741824)

#ifndef FIO
const char* Logger::logpath = "/work/kamiya/log.dat";
#else
const char* Logger::logpath = "/dev/fioa";
static uint32_t global_lsn;
#endif



/* 
   LogBuffer:
   
   まとめてwriteする必要があるので予めLogの配列を用意しておく。
   現在何個Logを所持しているかを管理する必要がある。
*/
class LogBuffer{
 private:
  static const unsigned MAX_LOG_SIZE=128;
  unsigned idx;
  int log_fd;

 public:
  Log logs[MAX_LOG_SIZE];
  std::mutex log_mtx;
  unsigned num_commit;
  int th_id;

  LogBuffer(){
    clear();
    log_fd = open(Logger::logpath, O_CREAT | O_RDWR | O_SYNC);
  }

  void
  clear(){
    idx=0;
    num_commit=0;
    memset(logs,0,sizeof(logs));
  }

  off_t
  offset()
  {
    return lseek(log_fd, 0, SEEK_CUR);
  }
  

  /*
    logHeaderはbaseアドレスに位置し、Logがその後に続く
   */
  void 
  flush()
  {
    LogHeader lh;
    off_t base = (off_t)this->th_id * LOG_OFFSET;
    
    lseek(log_fd, base, SEEK_SET);
    int ret = read(log_fd, &lh, sizeof(LogHeader));
    if(-1 == ret){
      perror("read"); exit(1);
    }
 
    uint64_t save_count = lh.count;
    lh.count += size();

    lseek(log_fd, base, SEEK_SET);
    if(-1 == write(log_fd, &lh, sizeof(LogHeader))){;
      perror("write"); exit(1);
    }


    off_t pos = base + sizeof(LogHeader) + (save_count * sizeof(Log));

    lseek(log_fd, pos, SEEK_SET);
    // lseek(log_fd, 0, SEEK_END);
    // fusionではSEEK_ENDできない

    write(log_fd, logs, sizeof(Log)*size());  

    // std::cout << "LogBuffer[" << th_id << "] flush " << size() << " logs from " << pos << "." << std::endl;
    
    clear();
  }

  size_t size(){
    return idx;
  }
  bool full(){
    return idx == MAX_LOG_SIZE;
  }
  bool empty(){
    return idx == 0;
  }
  void push(const Log &log){
    if(size() >= MAX_LOG_SIZE){
      perror("push");
      exit(1);
    }

    logs[idx] = log;
    idx++;

    if(log.Type == END)
      num_commit++;

    //    std::cout << "idx: " << idx << std::endl;
  }

  off_t next_lsn(){
#ifndef FIO
    off_t pos;
    LogHeader lh;
    
    lseek(log_fd, th_id*LOG_OFFSET, SEEK_SET);

    int ret = read(log_fd, &lh, sizeof(LogHeader));
    if(-1 == ret){
      perror("read");
      exit(1);
    }
    
    pos = sizeof(LogHeader) + (lh.count + size()) * sizeof(Log);
    return pos;

#else

    int old, new_val;
    do {
      old = global_lsn;
      new_val = old+1;
    }while(!CAS(&global_lsn, old, new_val));
  
    return new_val;

#endif    

  }
  
};

static LogBuffer logBuffer[NUM_MAX_CORE];



std::ostream& operator<<( std::ostream& os, OP_TYPE& opt){
  switch(opt){
  case NONE: os << "NONE"; break;
  case INC: os << "INC"; break;
  case DEC: os << "DEC"; break;
  case SUBST: os << "SUBST"; break;
  }

  return os;
}

std::ostream& operator<<( std::ostream& os, LOG_TYPE& type){
  //  UPDATE, COMPENSATION, PREPARE, END, OSfile_return, BEGIN };
  switch(type){
  case UPDATE: os << "UPDATE"; break;
  case COMPENSATION: os << "COMPENSATION"; break;
  case PREPARE: os << "PREPARE"; break;
  case BEGIN: os << "BEGIN"; break;
  case END: os << "END"; break;
  case OSfile_return: os << "OSfile_return"; break;
  }

  return os;
}


/* 各logBufferにth_idを指定する*/
void
Logger::init(){
  for(int i=0;i<MAX_CORE_NUM;i++)
    logBuffer[i].th_id = i;
}

int 
Logger::log_write(Log *log, int th_id){
#ifndef FIO
  th_id = 0;
#endif
  std::lock_guard<std::mutex> lock(logBuffer[th_id].log_mtx);  

  log->LSN = logBuffer[th_id].next_lsn();
  logBuffer[th_id].push(*log);

  if( (log->Type == END && logBuffer[th_id].num_commit == NUM_GROUP_COMMIT ) || logBuffer[th_id].full() ){
    logBuffer[th_id].flush();
  }

  return 0;
}

void
Logger::log_all_flush(){
  for(int i=0;i<NUM_MAX_CORE;i++)
    if(!logBuffer[i].empty())
      logBuffer[i].flush();
}

void 
Logger::log_debug(Log log){
  std::cout << "LSN: " << log.LSN;
  std::cout << ", TransID " << log.TransID;
  std::cout << ", Type: " << log.Type;
  std::cout << ", PageID: " << log.PageID;
  std::cout << ", PrevLSN: " << log.PrevLSN;
  std::cout << ", UndoNxtLSN: " << log.UndoNxtLSN;
  if(log.Type == UPDATE){
    std::cout << ", before: " << log.before;
    std::cout << ", after: " << log.after;
    //    std::cout << ", op.op_type: " << log.op.op_type;
    //    std::cout << ", op.amount: " << log.op.amount ;
  }
  std::cout << std::endl;
}



