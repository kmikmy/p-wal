#include "include/ARIES.h"
#include <iostream>
#include <utility>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <mutex>
#include <cstring>

using namespace std;

// #define DEBUG

/* 
   Group Commit の際のパラメータ. Defaultは1.
   Logger::set_num_group_commit()でセットする.
*/
uint32_t num_group_commit = 1; 

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
  static const unsigned MAX_CHUNK_LOG_SIZE=256*1024;;
  unsigned ptr_on_chunk; 
  int log_fd;
  off_t segment_base_addr;
  int th_id;
  int double_buffer_flag; // this flag's value is 0 or 1.

 public:
  LogSegmentHeader *log_segment_header[2];
  char *log_buffer_body[2];
  std::mutex mtx_for_insert;
  std::mutex mtx_for_write;
  unsigned num_commit;

  LogBuffer(){
    log_fd = open(Logger::logpath, O_CREAT | O_RDWR | O_DIRECT, 0666);
  }

  void
  clear_log(int double_buffer_flag){
    memset(log_buffer_body[d_flag], 0, MAX_CHUNK_SIZE);
    ptr=sizeof(size_t); // 先頭からsizeof(size_t)バイトは、chunkSizeを書き込む
    num_commit=0;
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
    segment_base_addr = (off_t)th_id * LOG_OFFSET;
    lseek(log_fd, segment_base_addr, SEEK_SET);

    /* ダブルバッファリング用にチャンクログ領域とログファイルヘッダを二つ所持している */
    for(int i=0;i<2;i++){
      if ((posix_memalign((void **) &log_buffer_body[i], 512, MAX_CHUNK_LOG_SIZE)) != 0) {
	fprintf(stderr, "posix_memalign failed\n");
	exit(1);
      }

      if ((posix_memalign((void **) &log_segment_header[i], 512, sizeof(LogSegmentHeader))) != 0) {
	fprintf(stderr, "posix_memalign failed\n");
	exit(1);
      }
    }
    
    /* 初めに使うチャンクログ領域とログファイルヘッダを初期化する */
    clear_log(0);
    int ret = read(log_fd, log_segment_header[0], sizeof(LogSegmentHeader));
    if(-1 == ret){
	cout << "log file open error" << endl;
      perror("read"); exit(1);
    }
  }

  /* 
     double buffering と chunksizeのアラインメントの操作を含む。
     この関数はinsertロックを保持した状態で呼ばれ。
     この関数内部でinsertロックを開放する。
     すなわち、この関数を呼び出した側で二重にinsertロックを開放してはならない。
   */
  void
  flush(){
    int _double_buffer_flag = getDoubleBufferFlag();
    size_t _chunk_size = getChunkSize();

    /* チャンクを切り替え */
    toggle_chunk();

    mtx_for_write.lock();
    mtx_for_insert.unlock();  
    /* この時点で、切り替わったチャンクにログを挿入可能 */
    
    nolock_flush(_double_buffer_flag, _chunk_size);
    mtx_for_write.unlock();
  }

  void
  toggle_chunk(){
    toggle_buffer();
    log_segment_header[double_buffer_flag] = log_segment_header[!double_buffer_flag];
    clear_log(double_buffer_flag);
  }

  /*
    logHeaderはsegment_base_addrに位置し、Logがその後に続く
   */
  void 
  nolock_flush(int _select_buffer, size_t _chunk_size)
  {
    uint64_t _file_size_before = log_segment_header[_select_buffer]->fileSize;
    log_segment_header[_select_buffer]->fileSize += _chunk_size;

    
    /* チャンクサイズをチャンクの先頭に記録 */
    memcpy(log_buffer_body, &_chunk_size, sizeof(_chunk_size));


    /* チャンクログの書き込み開始地点にlseek */
    off_t write_pos = segment_base_addr + fileSizeBefore; 
    lseek(log_fd, pos, SEEK_SET);

    if(-1 == write(log_fd, log_buffer_body[_select_buffer], _chunk_size)){ 
      perror("write(ChunkLog)"); exit(1);
    }
  
    /* ログセグメントの先頭(ログセグメントヘッダの買い込み開始地点)にlseek */
    lseek(log_fd, segment_base_addr, SEEK_SET);
    if(-1 == write(log_fd, log_segment_header[_select_buffer], sizeof(LogSegmentHeader))){
      perror("write(LogSegmentHeader)"); exit(1);
    }
    fsync(log_fd);
  }

  int 
  getDoubleBufferFlag(){
    return double_buffer_flag;
  }

  void
  toggle_buffer(){
    double_buffer_flag = double_buffer_flag?0:1; 
  }

  size_t
  getChunkSize(){
    size_t _chunk_size = getPtrOnChunk();
    if(_chunk_size % 512 != 0){
      _chunk_size += (512 - _chunk_size % 512);
    }
    return _chunk_size;
  }

  size_t
  getPtrOnChunk(){
    return ptr_on_chunk;
  }

  bool
  ableToAdd(size_t lsize){
    return ptr_on_chunk + lsize <=  MAX_CHUNK_LOG_SIZE;
  }

  bool
  empty(){
    return ptr_on_chunk == sizeof(size_t);
  }

  void
  push(Log *_log, FieldLogList *_field_log_list){
    FieldLogList *p;
    
    if(getPtrOnChunk() + _log->totalLength >= MAX_CHUNK_SIZE){
      perror("can't push");
      exit(1);
    }
    memcpy(&log_buffer_body[double_buffer_flag][ptr_on_chunk], _log, sizeof(Log));
    ptr_on_chunk += sizeof(Log);

    for(p = _fieldLogList; p != NULL; p = p->nxt){ // fieldLogListが最初からNULLの場合は何もしない
      memcpy(&log_buffer_body[double_buffer_flag][ptr_on_chunk], p, sizeof(size_t)*2); // fieldOffsetとfieldLengthのcopy
      ptr_on_chunk += sizeof(size_t)*2;
      memcpy(&log_buffer_body[double_buffer_flag][ptr_on_chunk], p->before, p->fieldLength);
      ptr_on_chunk += p->fieldLength;
      memcpy(&log_buffer_body[double_buffer_flag][ptr_on_chunk], p->after, p->fieldLength);
      ptr_on_chunk += p->fieldLength;
    }

    if(_log->type == END)
      num_commit++;

    //    std::cout << "ptr_on_chunk: " << ptr_on_chunk << std::endl;
  }

  LSN_and_Offset
  next_lsn_and_offset(){
    LSN_and_Offset _ret;
    off_t _pos = next_offset();
    
    _ret.second = _pos;

#ifdef FIO
    _ret.first = __sync_fetch_and_add(&ARIES_SYSTEM::master_record.system_last_lsn, 1);
#else
    /* この時点で既にインサートロックを獲得している */
    _ret.first = _pos; // Normal WALではLSNとoffsetは等しい
    ARIES_SYSTEM::master_record.system_last_lsn = _pos; // システムのGlobalLSNの更新
#endif    

    return _ret;
  }
  
  uint64_t
  next_offset(){
    uint64_t pos = segment_base_addr + log_segment_header[getDoubleBufferFlag()]->fileSize + getPtrOnChunk();
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
Logger::log_write(Log *_log, FieldLogList *_field_log_list, int _th_id){
  LSN_and_Offset _lsn_and_offset;
  bool _push_failed = true;

#ifndef FIO
  _th_id = 0;
#endif
  logBuffer[_th_id].mtx_for_insert.lock();  



  _lsn_and_offset = logBuffer[_th_id].next_lsn_and_offset();
  _log->lsn = _lsn_and_offset.first;
  _log->offset = _lsn_and_offset.second;
  _log->fileId = _th_id;
  
  do(_push_success){
    if(logBuffer[_th_id].ableToAdd(log->totalLength)){
      logBuffer[_th_id].push(log, fieldLogList);
    } else {
      logBuffer[_th_id].flush(); // flush()の中でinsertロックを開放している
      logBuffer[_th_id].mtx_for_insert.lock(); // 再度insertロックの獲得を試みる
      _push_failed = false;
    }
  }while(_push_failed);

  if( log->type == END && logBuffer[th_id].num_commit == NUM_GROUP_COMMIT ){
    logBuffer[th_id].flush(); // flush()の中でinsertロックを開放している
    return 0;
  }

  logBuffer[_th_id].mtx_for_insert.unlock();  
  return 0;
}


/* ログバッファのフラッシュを行う */
void
Logger::log_flush(int th_id){
#ifndef FIO
  th_id = 0;
#endif
  logBuffer[th_id].mtx_for_insert.lock();  
  logBuffer[th_id].flush(); // insertロックはLogger.flush()の中で開放される
}

/* 全てのログバッファのフラッシュを行う(システム終了時) */
void
Logger::log_all_flush(){
  for(int i=0;i<MAX_WORKER_THREAD;i++){
    if(!logBuffer[i].empty()){
      logBuffer[i].flush();
    }
  }
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

