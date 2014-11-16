#include "ARIES.h"
#include <iostream>
#include <utility>

using namespace std;

#define CAS(addr, oldval, newval) __sync_bool_compare_and_swap(addr, oldval, newval)
#define CAS64(addr, oldval, newval) __sync_bool_compare_and_swap((long *)addr, *(long *)&oldval, *(long *)&newval
#define CAS128(addr, oldval, newval) __sync_bool_compare_and_swap((__int128_t *)(addr), *(__int128_t *)&(oldval), *(__int128_t *)&(newval))
// #define DEBUG

/* 
   Group Commit の際のパラメータ. Defaultは1.
   Logger::set_num_group_commit()でセットする.
*/
uint32_t num_group_commit = 1; 

#ifndef FIO
const char* Logger::logpath = "/dev/fioa";
#else
const char* Logger::logpath = "/dev/fioa";
//const char* Logger::logpath = "/dev/shm/kamiya/log.dat";
#endif

typedef pair<off_t, off_t> LSN_and_Offset;

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
  off_t base_addr;
  LogHeader header;
  int th_id;

 public:
  Log logs[MAX_LOG_SIZE];
  std::mutex log_mtx;
  unsigned num_commit;

  LogBuffer(){
    clear();
    log_fd = open(Logger::logpath, O_CREAT | O_RDWR | O_SYNC, 0666);
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
  init(int _th_id)
  {
    th_id = _th_id;
    base_addr = (off_t)th_id * LOG_OFFSET;
    lseek(log_fd, base_addr, SEEK_SET);
    int ret = read(log_fd, &header, sizeof(LogHeader));
    if(-1 == ret){
      perror("read"); exit(1);
    }
  }

  /*
    logHeaderはbase_addrに位置し、Logがその後に続く
   */
  void 
  flush()
  {
    uint64_t save_count = header.count;
    header.count += size();

    lseek(log_fd, base_addr, SEEK_SET);
    if(-1 == write(log_fd, &header, sizeof(LogHeader))){;
      perror("write"); exit(1);
    }

    off_t pos = base_addr + sizeof(LogHeader) + (save_count * sizeof(Log));

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

  LSN_and_Offset next_lsn(){
    LSN_and_Offset ret;
    off_t pos;
    
    pos = base_addr + sizeof(LogHeader) + (header.count + size()) * sizeof(Log);
    ret.second = pos;

#ifndef FIO
    ret.first = pos;
    ARIES_SYSTEM::master_record.system_last_lsn = pos;
#else
    int old, new_val;
    do {
      old = ARIES_SYSTEM::master_record.system_last_lsn;
      new_val = old+1;
    }while(!CAS(&ARIES_SYSTEM::master_record.system_last_lsn, old, new_val));
    
    ret.first = new_val;
#endif    

    return ret;
  }
  
  uint64_t
  next_offset(){
    uint64_t pos = base_addr + sizeof(LogHeader) + (header.count + size()) * sizeof(Log);
    return pos;
  }

};

static LogBuffer logBuffer[MAX_WORKER_THREAD];

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


/* 各logBufferにth_idを設定して、ヘッダーを読み込む*/
void
Logger::init(){
  for(int i=0;i<MAX_WORKER_THREAD;i++){
    logBuffer[i].init(i);
  }
}

int 
Logger::log_write(Log *log, int th_id){
#ifndef FIO
  th_id = 0;
#endif
  std::lock_guard<std::mutex> lock(logBuffer[th_id].log_mtx);  

  LSN_and_Offset lao;

  lao = logBuffer[th_id].next_lsn();
  log->LSN = lao.first;
  log->Offset = lao.second;
  log->file_id = th_id;

  logBuffer[th_id].push(*log);

  if( (log->Type == END && logBuffer[th_id].num_commit == num_group_commit ) || logBuffer[th_id].full() ){
    logBuffer[th_id].flush();
  }

  return 0;
}

void
Logger::log_all_flush(){
  for(int i=0;i<MAX_WORKER_THREAD;i++)
    if(!logBuffer[i].empty())
      logBuffer[i].flush();
}

uint64_t
Logger::read_LSN(){
  return ARIES_SYSTEM::master_record.system_last_lsn;
}

uint64_t
Logger::current_offset_logfile_for_id(int th_id){
  return logBuffer[th_id].next_offset();
}

void 
Logger::log_debug(Log log){
  std::cout << "Log[" << log.LSN;
  std::cout << "," << log.Offset ;
  std::cout << "]: TransID: " << log.TransID;
  std::cout << ", file_id: " << log.file_id;
  std::cout << ", Type: " << log.Type;
  std::cout << ", PageID: " << log.PageID;
  std::cout << ", PrevLSN: " << log.PrevLSN;
  std::cout << ", UndoNxtLSN: " << log.UndoNxtLSN;
  if(log.Type == UPDATE || log.Type == COMPENSATION){
    std::cout << ", before: " << log.before;
    std::cout << ", after: " << log.after;
    //    std::cout << ", op.op_type: " << log.op.op_type;
    //    std::cout << ", op.amount: " << log.op.amount ;
  }
  std::cout << std::endl;
}

void
Logger::set_num_group_commit(int group_param){
  num_group_commit = group_param;
}

