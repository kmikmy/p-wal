#include "include/ARIES.h"
#include "include/log.h"
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
  static const unsigned MAX_CHUNK_LOG_SIZE=256*1024;
  unsigned ptr_on_chunk_;
  int log_fd;
  off_t segment_base_addr;
  int th_id;
  int buffer_id_; // this flag's value is 0 or 1.

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
  clear_log(int buffer_id){
    memset(log_buffer_body[buffer_id], 0, MAX_CHUNK_LOG_SIZE);
    ptr_on_chunk_=sizeof(size_t); // 先頭からsizeof(size_t)バイトは、chunkSizeを書き込む
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
    buffer_id_ = 0;
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
    int buffer_id_ = getDoubleBufferFlag();
    size_t _chunk_size = getChunkSize();

    /* チャンクを切り替え */
    toggle_chunk();

    mtx_for_write.lock();
    mtx_for_insert.unlock();
    /* この時点で、切り替わったチャンクにログを挿入可能 */

    nolock_flush(buffer_id_, _chunk_size);
    mtx_for_write.unlock();
  }

  void
  toggle_chunk(){
    toggle_buffer();
    log_segment_header[buffer_id_] = log_segment_header[!buffer_id_];
    clear_log(buffer_id_);
  }

  /*
    logHeaderはsegment_base_addrに位置し、Logがその後に続く
   */
  void
  nolock_flush(int _select_buffer, size_t _chunk_size)
  {
    uint64_t file_size_before = log_segment_header[_select_buffer]->fileSize;
    log_segment_header[_select_buffer]->fileSize += _chunk_size;

    /* チャンクサイズをチャンクの先頭に記録 */
    memcpy(log_buffer_body, &_chunk_size, sizeof(_chunk_size));


    /* チャンクログの書き込み開始地点にlseek */
    off_t write_pos = segment_base_addr + file_size_before;
    lseek(log_fd, write_pos, SEEK_SET);

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
    return buffer_id_;
  }

  void
  toggle_buffer(){
    buffer_id_ = buffer_id_?0:1; 
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
    return ptr_on_chunk_;
  }

  bool
  ableToAdd(size_t lsize){
    return ptr_on_chunk_ + lsize <=  MAX_CHUNK_LOG_SIZE;
  }

  bool
  empty(){
    return ptr_on_chunk_ == sizeof(size_t);
  }

  void
  push(Log *log, FieldLogList *field_log_list){
    FieldLogList *p;

    if(getPtrOnChunk() + log->total_length >= MAX_CHUNK_LOG_SIZE){
      perror("can't push");
      exit(1);
    }
    memcpy(&log_buffer_body[buffer_id_][ptr_on_chunk_], log, sizeof(Log));
    ptr_on_chunk_ += sizeof(Log);

    for(p = field_log_list; p != NULL; p = p->nxt){ // fieldLogListが最初からNULLの場合は何もしない
      memcpy(&log_buffer_body[buffer_id_][ptr_on_chunk_], p, sizeof(size_t)*2); // fieldOffsetとfieldLengthのcopy
      ptr_on_chunk_ += sizeof(size_t)*2;
      memcpy(&log_buffer_body[buffer_id_][ptr_on_chunk_], p->before, p->fieldLength);
      ptr_on_chunk_ += p->fieldLength;
      memcpy(&log_buffer_body[buffer_id_][ptr_on_chunk_], p->after, p->fieldLength);
      ptr_on_chunk_ += p->fieldLength;
    }

    if(log->type == END)
      num_commit++;

    //    std::cout << "ptr_on_chunk_: " << ptr_on_chunk_ << std::endl;
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

std::ostream& operator<<( std::ostream& os, kLogType& type){
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
Logger::log_write(Log *log, FieldLogList *field_log_list, int th_id){
  LSN_and_Offset lsn_and_offset;
  bool try_push = true;

#ifndef FIO
  th_id = 0;
#endif
  logBuffer[th_id].mtx_for_insert.lock();

  lsn_and_offset = logBuffer[th_id].next_lsn_and_offset();
  log->lsn = lsn_and_offset.first;
  log->offset = lsn_and_offset.second;
  log->file_id = th_id;

  while(try_push){
    if(logBuffer[th_id].ableToAdd(log->total_length)){
      logBuffer[th_id].push(log, field_log_list);
    } else {
      logBuffer[th_id].flush(); // flush()の中でinsertロックを開放している
      logBuffer[th_id].mtx_for_insert.lock(); // 再度insertロックの獲得を試みる
      try_push = false;
    }
  }

  if( log->type == END && logBuffer[th_id].num_commit == num_group_commit ){
    logBuffer[th_id].flush(); // flush()の中でinsertロックを開放している
    return 0;
  }

  logBuffer[th_id].mtx_for_insert.unlock();
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
  std::cout << "Log[" << log.lsn;
  std::cout << "," << log.offset ;
  std::cout << "]: trans_id: " << log.trans_id;
  std::cout << ", file_id: " << log.file_id;
  std::cout << ", type: " << log.type;
  std::cout << ", page_id: " << log.page_id;
  std::cout << ", prev_lsn: " << log.prev_lsn;
  std::cout << ", undo_nxt_lsn: " << log.undo_nxt_lsn;
  if(log.type == UPDATE || log.type == COMPENSATION){
    // フィールドを表示する
    //    std::cout << ", before: " << log.before;
    //    std::cout << ", after: " << log.after;
    //    std::cout << ", op.op_type: " << log.op.op_type;
    //    std::cout << ", op.amount: " << log.op.amount ;
  }
  std::cout << std::endl;
}

void
Logger::set_num_group_commit(int group_param){
  num_group_commit = group_param;
}

uint32_t Logger::num_group_commit = 1;
