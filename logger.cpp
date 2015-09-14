#include "include/ARIES.h"
#include <iostream>
#include <utility>
#include <sys/time.h>
#include <unistd.h>

using namespace std;

// #define DEBUG

/* 
   Group Commit の際のパラメータ. Defaultは1.
   Logger::set_num_group_commit()でセットする.
*/
uint32_t num_group_commit = 1; 
uint64_t cas_miss[MAX_WORKER_THREAD];

#ifdef FIO
const char* Logger::logpath = "/dev/fioa";
#else
const char* Logger::logpath = "/dev/fioa";
//const char* Logger::logpath = "/work2/tmp/log.dat";
#endif

typedef pair<off_t, off_t> LSN_and_Offset;

/* 
   LogBuffer:
   
   まとめてwriteする必要があるので予めLogの配列を用意しておく。
   現在何個Logを所持しているかを管理する必要がある。
*/
class LogBuffer{
 private:
  static const unsigned MAX_LOG_SIZE=512;
  unsigned idx;
  int log_fd;
  off_t base_addr;
  int th_id;
  int double_buffer_flag; // this flag's value is 0 or 1.

 public:
  //  Log logs[2][MAX_LOG_SIZE];
  Log *logs[2];
  //  LogHeader *header;
  LogHeader *header;
  std::mutex mtx_for_insert;
  std::mutex mtx_for_write;
  unsigned num_commit;

  LogBuffer(){
    clear();
    log_fd = open(Logger::logpath, O_CREAT | O_RDWR | O_DIRECT, 0666);
  }

  void
  clear(){
    idx=0;
    num_commit=0;
    //    memset(logs,0,sizeof(logs));
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
    double_buffer_flag = 0;
    base_addr = (off_t)th_id * LOG_OFFSET;
    lseek(log_fd, base_addr, SEEK_SET);

    if ((posix_memalign((void **) &logs[0], 512, sizeof(Log)*MAX_LOG_SIZE)) != 0) {
      fprintf(stderr, "posix_memalign failed\n");
      exit(1);
    }
    if ((posix_memalign((void **) &logs[1], 512, sizeof(Log)*MAX_LOG_SIZE)) != 0) {
      fprintf(stderr, "posix_memalign failed\n");
      exit(1);
    }

    if ((posix_memalign((void **) &header, 512, sizeof(LogHeader))) != 0) {
      fprintf(stderr, "posix_memalign failed\n");
      exit(1);
    }
    int ret = read(log_fd, header, sizeof(LogHeader));
    if(-1 == ret){
	cout << "log file open error" << endl;
      perror("read"); exit(1);
    }
  }

  /*
    logHeaderはbase_addrに位置し、Logがその後に続く
   */
  void 
  flush(int flag, LogHeader *header, size_t nlog)
  {
    uint64_t save_count = header->count;
    header->count += nlog;

    lseek(log_fd, base_addr, SEEK_SET);
    if(-1 == write(log_fd, header, sizeof(LogHeader))){;
      perror("write(LogHeader)"); exit(1);
    }

    off_t pos = base_addr + sizeof(LogHeader) + (save_count * sizeof(Log));

    //    cout << "flush(): base_addr=" << base_addr << ", pos=" << pos << endl;

    lseek(log_fd, pos, SEEK_SET);
    // lseek(log_fd, 0, SEEK_END);
    // fusionではSEEK_ENDできない
    
    if(-1 == write(log_fd, logs[flag], sizeof(Log)*nlog)){
      perror("write(Log)"); exit(1);
    }
    fsync(log_fd);

    // std::cout << "LogBuffer[" << th_id << "] flush " << size() << " logs from " << pos << "." << std::endl;
    
    //    clear();
  }


  int getDoubleBufferFlag(){
    return double_buffer_flag;
  }
  size_t getSize(){
    return idx;
  }
  bool full(){
    return idx == MAX_LOG_SIZE;
  }
  bool empty(){
    return idx == 0;
  }
  void push(const Log &log){
    if(getSize() >= MAX_LOG_SIZE){
      perror("push");
      exit(1);
    }

    logs[double_buffer_flag][idx] = log;
    idx++;

    if(log.Type == END)
      num_commit++;

    //    std::cout << "idx: " << idx << std::endl;
  }

  LSN_and_Offset next_lsn(){
    LSN_and_Offset ret;
    off_t pos;
    
    pos = base_addr + sizeof(LogHeader) + (header->count + getSize()) * sizeof(Log);
    ret.second = pos;

#ifndef FIO
    ret.first = pos;
    ARIES_SYSTEM::master_record.system_last_lsn = pos;
#else
    ret.first = __sync_fetch_and_add(&ARIES_SYSTEM::master_record.system_last_lsn,1);
    // int old, new_val;
    // do {
    //   old = ARIES_SYSTEM::master_record.system_last_lsn;
    //   new_val = old+1;
    //   if(CAS(&ARIES_SYSTEM::master_record.system_last_lsn, old, new_val))
    // 	break;
    //   else
    // 	cas_miss[th_id]++;
    // }while(1);
#endif    

    return ret;
  }
  
  uint64_t
  next_offset(){
    uint64_t pos = base_addr + sizeof(LogHeader) + (header->count + getSize()) * sizeof(Log);
    return pos;
  }

  void
  toggle_buffer(){
    double_buffer_flag = double_buffer_flag?0:1; 
  }

};

static LogBuffer logBuffer[MAX_WORKER_THREAD];

std::ostream& operator<<( std::ostream& os, OP_TYPE& opt){
  switch(opt){
  case NONE: os << "NONE"; break;
  case INC: os << "INC"; break;
  case DEC: os << "DEC"; break;
  case SUBST: os << "SUBST"; break;
  case READ: os << "READ"; break;
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
  case INSERT: os << "INSERT"; break;
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
  logBuffer[th_id].mtx_for_insert.lock();  
#endif

  LSN_and_Offset lao;

  lao = logBuffer[th_id].next_lsn();
  log->LSN = lao.first;
  log->Offset = lao.second;
  log->file_id = th_id;

  logBuffer[th_id].push(*log);

  if( (log->Type == END && logBuffer[th_id].num_commit == num_group_commit ) || logBuffer[th_id].full() ){

    int tmp_flag = logBuffer[th_id].getDoubleBufferFlag();
    //    LogHeader *tmp_header = logBuffer[th_id].header;
    LogHeader *tmp_header;
    size_t tmp_size = logBuffer[th_id].getSize();

    if ((posix_memalign((void **) &tmp_header, 512, sizeof(LogHeader))) != 0)
      {
        fprintf(stderr, "posix_memalign failed\n");
	exit(1);
      }

    *tmp_header = *(logBuffer[th_id].header);

    /* ログバッファを切り替えて、ヘッダを更新 */
    logBuffer[th_id].toggle_buffer();
    logBuffer[th_id].header->count += tmp_size;
    logBuffer[th_id].clear(); // idxとnum_commitの値を初期化する

#ifndef FIO
    logBuffer[th_id].mtx_for_write.lock();
    logBuffer[th_id].mtx_for_insert.unlock();  
#endif

    logBuffer[th_id].flush(tmp_flag, tmp_header, tmp_size);
#ifndef FIO
    logBuffer[th_id].mtx_for_write.unlock();
#endif

    return 0;
  }

#ifndef FIO
  logBuffer[th_id].mtx_for_insert.unlock();  
#endif

  return 0;
}


/* insert lockを獲得した後、write_lockを獲得して、強制的にflushする */
void
Logger::log_flush(int th_id){
#ifndef FIO
  th_id = 0;
  logBuffer[th_id].mtx_for_insert.lock();  
#endif

  int tmp_flag = logBuffer[th_id].getDoubleBufferFlag();
  //    LogHeader *tmp_header = logBuffer[th_id].header;
  LogHeader *tmp_header;
  size_t tmp_size = logBuffer[th_id].getSize();

  if ((posix_memalign((void **) &tmp_header, 512, sizeof(LogHeader))) != 0)
    {
      fprintf(stderr, "posix_memalign failed\n");
      exit(1);
    }

  *tmp_header = *(logBuffer[th_id].header);

  /* ログバッファを切り替えて、ヘッダを更新 */
  logBuffer[th_id].toggle_buffer();
  logBuffer[th_id].header->count += tmp_size;
  logBuffer[th_id].clear(); // idxとnum_commitの値を初期化する

#ifndef FIO
  logBuffer[th_id].mtx_for_write.lock();
  logBuffer[th_id].mtx_for_insert.unlock();  
#endif

  logBuffer[th_id].flush(tmp_flag, tmp_header, tmp_size);
#ifndef FIO
  logBuffer[th_id].mtx_for_write.unlock();
#endif
}


/* lockを獲得しないので、ワーカが競合する時には使えない */
void
Logger::log_all_flush(){
  for(int i=0;i<MAX_WORKER_THREAD;i++)
    if(!logBuffer[i].empty())
      logBuffer[i].flush(logBuffer[i].getDoubleBufferFlag(), logBuffer[i].header, logBuffer[i].getSize());
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

