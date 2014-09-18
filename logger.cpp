#include "ARIES.h"
#include <iostream>

#define NUM_GROUP_COMMIT 10

#ifndef FIO
const char* Logger::logpath = "/work/kamiya/log.dat";
#else
const char* Logger::logpath = "/dev/fioa";
const uint32_t log_offset = 1073741824; // 1GB
#endif

static int log_fd;


/* 
   LogBuffer:
   
   まとめてwriteする必要があるので予めLogの配列を用意しておく。
   現在何個Logを所持しているかを管理する必要がある。
*/
class LogBuffer{
 private:
  static const unsigned MAX_LOG_SIZE=128;
  unsigned idx;


 public:
  Log logs[MAX_LOG_SIZE];
  unsigned num_commit;

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
  

  void 
  flush()
  {
    LogHeader lh;
    
    lseek(log_fd, 0, SEEK_SET);
    if(-1 == read(log_fd, &lh, sizeof(LogHeader))){
      perror("read"); exit(1);
    };
    unsigned save_count = lh.count;
    lh.count += size();
  
    lseek(log_fd, 0, SEEK_SET);
    if(-1 == write(log_fd, &lh, sizeof(LogHeader))){;
      perror("write"); exit(1);
    }

    off_t pos = sizeof(LogHeader) + save_count * sizeof(Log);
    lseek(log_fd, pos, SEEK_SET);
    // lseek(log_fd, 0, SEEK_END);
    // fusionではSEEK_ENDできない

    write(log_fd, logs, sizeof(Log)*size());  

    //  std::cout << "flush" << std::endl;
    
    clear();
  }
  

  size_t size(){
    return idx;
  }
  bool full(){
    return idx == MAX_LOG_SIZE;
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
    off_t pos;
    LogHeader lh;
    
    lseek(log_fd, 0, SEEK_SET);
    if(-1 == read(log_fd, &lh, sizeof(LogHeader))){
      perror("read");
      exit(1);
    }
    
    pos = sizeof(LogHeader) + (lh.count + size()) * sizeof(Log);
    return pos;
    
  }
  
};

static LogBuffer logBuffer;
static std::mutex log_mtx;

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

int Logger::log_write(Log *log){
  std::lock_guard<std::mutex> lock(log_mtx);  

  //  std::cout << logBuffer.next_lsn() << std::endl;
  log->LSN = logBuffer.next_lsn();
  logBuffer.push(*log);

  if( (log->Type == END && logBuffer.num_commit == NUM_GROUP_COMMIT ) || logBuffer.full() ){
    log_flush();
  }

  return 0;
}

void 
Logger::log_flush()
{
  logBuffer.flush();
}


void Logger::log_debug(Log log){
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



  

