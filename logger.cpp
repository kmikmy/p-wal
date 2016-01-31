#include "include/ARIES.h"
#include "include/log.h"
#include <nvm/nvm_primitives.h>
#include <nvm/nvm_types.h>
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

#define _NVM
#ifdef _NVM
/******************************************************************************
 * These macros correspond to the NVM_PRIMITIVES_API_* version macros for
 * the library version for which this code was designed/written/modified.
 * Whenever the code is changed to work with a newer library version, modify
 * the below values too. The values should be taken from the nvm_primitives.h
 * file for the version of the library used when this code was written/modified
 * and compiled.
 ****************************************************************************/
//Major version of NVM library used when compiling this code
const int  NVM_MAJOR_VERSION_USED_COMPILATION=1;
//Minor version of NVM library used when compiling this code
const int  NVM_MINOR_VERSION_USED_COMPILATION=0;
//Micro version of NVM library used when compiling this code
const int  NVM_MICRO_VERSION_USED_COMPILATION=0;

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

#ifdef _NVM
  nvm_handle_t handle = -1;
  nvm_iovec_t *iov = NULL;
#endif

 public:
  ChunkLogHeader chunk_log_header[2];
  uint64_t total_write_size;

  // O_DIRECTで読み書きするのでポインタで取得する
  // next_offsetの計算で読み込むため、ダブルバッファ用に二つ用意しなければならない
  LogSegmentHeader *log_segment_header[2];
  char *log_buffer_body[2];

  std::mutex mtx_for_insert;
  std::mutex mtx_for_write;
  unsigned num_commit;

  LogBuffer()
  {
    log_fd = open(Logger::logpath, O_CREAT | O_RDWR | O_DIRECT, 0666);
    if(log_fd == -1){
      cout << "log file open error" << endl;
      perror("open");
      exit(1);
    }
    total_write_size = 0;


#ifdef _NVM
    /**************************/
    /* Get the version_info   */
    /**************************/
    nvm_version_t                version_used_during_compilation;
    version_used_during_compilation.major = NVM_MAJOR_VERSION_USED_COMPILATION;
    version_used_during_compilation.minor = NVM_MINOR_VERSION_USED_COMPILATION;
    version_used_during_compilation.micro = NVM_MICRO_VERSION_USED_COMPILATION;

    /*************************/
    /* Get the file handle   */
    /*************************/
    handle = nvm_get_handle(log_fd, &version_used_during_compilation);
    if (handle < 0){
      PERR("nvm_get_handle: failed");
    }

#endif
  }

  void
  clearLog(int buffer_id)
  {
    //  memset(log_buffer_body[buffer_id], 0, MAX_CHUNK_LOG_SIZE);
    ptr_on_chunk_=sizeof(ChunkLogHeader);
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

#ifdef _NVM
    iov = (nvm_iovec_t *) malloc(sizeof(nvm_iovec_t) * 2);
    if (iov == NULL){
      PERR("malloc");
    }


    //    iov[0].iov_base is decided whether log_segment_header[0,1] when write()
    iov[0].iov_len    = 512;
    iov[0].iov_opcode = NVM_IOV_WRITE;
    iov[0].iov_lba    = segment_base_addr / 512;

    // iov[1].iov_vase, iov_len, iov_lba are decided when write()
    iov[1].iov_opcode = NVM_IOV_WRITE;
#endif


    /* 初めに使うチャンクログ領域を初期化する */
    clearLog(0);

    lseek(log_fd, segment_base_addr, SEEK_SET);
    int ret = read(log_fd, log_segment_header[0], sizeof(LogSegmentHeader));
    if(-1 == ret){
	cout << "log file read error" << endl;
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
    int buffer_id = getDoubleBufferFlag();

    mtx_for_write.lock();

    /* チャンクを切り替え */
    toggleChunk();

    mtx_for_insert.unlock();
    /* この時点で、切り替わったチャンクにログを挿入可能 */

    nolockFlush(buffer_id);
    mtx_for_write.unlock();
  }

  void
  updateSegmentAndChunkHeader(){
    int id = getDoubleBufferFlag();
    uint64_t csize = getChunkSize();

    chunk_log_header[id].chunk_size = csize;

    log_segment_header[id]->segment_size += csize;
    log_segment_header[id]->chunk_num++;
    log_segment_header[id]->log_record_num += chunk_log_header[id].log_record_num;
  }

  void
  toggleChunk()
  {
    updateSegmentAndChunkHeader();
    toggleBuffer();
    memcpy(log_segment_header[buffer_id_], log_segment_header[!buffer_id_], sizeof(LogSegmentHeader));
    chunk_log_header[buffer_id_].chunk_size = 0;
    chunk_log_header[buffer_id_].log_record_num = 0;
    clearLog(buffer_id_);
  }

  /*
    LogSegmentHeaderはsegment_base_addrに位置し、複数のChunkLogがその後に続く
   */
  void
  nolockFlush(int _select_buffer)
  {
    int id = _select_buffer;
    uint64_t chunk_head_pos = log_segment_header[id]->segment_size - chunk_log_header[id].chunk_size;


    /* チャンクの先頭にヘッダを記録 */
    memcpy( log_buffer_body[id], &chunk_log_header[id], sizeof(ChunkLogHeader));

    /* チャンクログの書き込み開始地点にlseek */
    off_t write_pos = segment_base_addr + chunk_head_pos;

#ifndef _NVM
    lseek(log_fd, write_pos, SEEK_SET);
    if( -1 == write( log_fd, log_buffer_body[id], chunk_log_header[id].chunk_size)){
      perror("write(ChunkLog)"); exit(1);
    }
    /* ログセグメントの先頭(ログセグメントヘッダの買い込み開始地点)にlseek */
    lseek(log_fd, segment_base_addr, SEEK_SET);
    if(-1 == write(log_fd, log_segment_header[id], sizeof(LogSegmentHeader))){
      perror("write(LogSegmentHeader)"); exit(1);
    }

    fsync(log_fd);
#else
    iov[0].iov_base   = (uint64_t)((uintptr_t) log_segment_header[id]);
    iov[1].iov_base   = (uint64_t)((uintptr_t) log_buffer_body[id]);
    iov[1].iov_len    = chunk_log_header[id].chunk_size;
    iov[1].iov_lba    = write_pos / 512;

    /**************************************/
    /* perform the batch atomic operation */
    /**************************************/

    int bytes_written = nvm_batch_atomic_operations(handle, iov, 2, 0);
    if (bytes_written < 0){
	PERR("nvm_batch_atomic_operations");
    }

#endif
    total_write_size += chunk_log_header[id].chunk_size;
    total_write_size += sizeof(LogSegmentHeader);

  }

  int
  getDoubleBufferFlag()
  {
    return buffer_id_;
  }

  void
  toggleBuffer()
  {
    buffer_id_ = buffer_id_? 0 : 1;
  }

  // パディングを含めたチャンクのサイズ
  size_t
  getChunkSize()
  {
    size_t _chunk_size = getPtrOnChunk();
    if(_chunk_size % 512 != 0){
      _chunk_size += (512 - _chunk_size % 512);
    }
    return _chunk_size;
  }

  // パディングを含めない現在のチャンクの実サイズ
  size_t
  getPtrOnChunk()
  {
    return ptr_on_chunk_;
  }

  bool
  ableToAdd(size_t lsize)
  {
    return ptr_on_chunk_ + lsize <=  MAX_CHUNK_LOG_SIZE;
  }

  bool
  empty()
  {
    return ptr_on_chunk_ == sizeof(ChunkLogHeader);
  }

  void
  push(Log *log, std::vector<FieldLogList> &bkpblock)
  {
    int id = getDoubleBufferFlag();
    int ptr = getPtrOnChunk();

    if(log->lsn == 0 || log->offset == 0){
      perror("log-data is broken! in push()");
      exit(1);
    }

    if(ptr + log->total_length >= MAX_CHUNK_LOG_SIZE){
      perror("can't push");
      exit(1);
    }
    memcpy(&log_buffer_body[id][ptr], log, sizeof(Log));
    ptr += sizeof(Log);

    for(unsigned i=0; i<bkpblock.size(); i++){
      memcpy(&log_buffer_body[id][ptr], &bkpblock[i], sizeof(FieldLogHeader)); // 更新フィールドのオフセットと長さ
      ptr += sizeof(FieldLogHeader);
      memcpy(&log_buffer_body[id][ptr], &bkpblock[i].before, bkpblock[i].field_length);
      ptr += bkpblock[i].field_length;
      memcpy(&log_buffer_body[id][ptr], &bkpblock[i].after, bkpblock[i].field_length);
      ptr += bkpblock[i].field_length;
    }

    ptr_on_chunk_ = ptr;

    if(log->type == END)
      num_commit++;

    chunk_log_header[id].log_record_num++;

    //    std::cout << "ptr: " << ptr_on_chunk_ << std::endl;
  }

  LSN_and_Offset
  nextLSNAndOffset()
  {
    LSN_and_Offset _ret;
    off_t _pos = nextOffset();

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
  nextOffset()
  {
    uint64_t pos = segment_base_addr + log_segment_header[getDoubleBufferFlag()]->segment_size + getPtrOnChunk();
    return pos;
  }
};

static LogBuffer logBuffer[MAX_WORKER_THREAD];

std::ostream&
operator<<( std::ostream& os, OP_TYPE& opt)
{
  switch(opt){
  case NONE: os << "NONE"; break;
  case INC: os << "INC"; break;
  case DEC: os << "DEC"; break;
  case SUBST: os << "SUBST"; break;
  case READ: os << "READ"; break;
  }

  return os;
}

std::ostream&
operator<<( std::ostream& os, kLogType& type)
{
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
Logger::init()
{
  for(int i=0;i<MAX_WORKER_THREAD;i++){
    logBuffer[i].init(i);
  }
}

int
Logger::logWrite(Log *log, std::vector<FieldLogList> &field_log_list, int th_id)
{
  LSN_and_Offset lsn_and_offset;
  bool try_push = true;
#ifndef FIO
  th_id = 0;
#endif


  logBuffer[th_id].mtx_for_insert.lock();

  lsn_and_offset = logBuffer[th_id].nextLSNAndOffset();
  log->lsn = lsn_and_offset.first;
  log->offset = lsn_and_offset.second;
  log->file_id = th_id;

  if(log->lsn == 0 || log->offset == 0){
    perror("log-data is broken! in logWrite()");
    exit(1);
  }

  while(try_push){
    if(logBuffer[th_id].ableToAdd(log->total_length)){
      logBuffer[th_id].push(log, field_log_list);
      try_push = false;
    } else {
      logBuffer[th_id].flush(); // flush()の中でinsertロックを開放している
      logBuffer[th_id].mtx_for_insert.lock(); // 再度insertロックの獲得を試みる
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
Logger::logFlush(int th_id)
{
#ifndef FIO
  th_id = 0;
#endif
  logBuffer[th_id].mtx_for_insert.lock();
  logBuffer[th_id].flush(); // insertロックはLogger.flush()の中で開放される
}

/* 全てのログバッファのフラッシュを行う(システム終了時) */
void
Logger::logAllFlush()
{
  for(int i=0;i<MAX_WORKER_THREAD;i++){
    if(!logBuffer[i].empty()){
      logBuffer[i].flush();
    }
  }
}

uint64_t
Logger::readLSN()
{
  return ARIES_SYSTEM::master_record.system_last_lsn;
}

uint64_t
Logger::currentOffsetLogFile(int th_id)
{
  return logBuffer[th_id].nextOffset();
}

void
Logger::logDebug(Log log)
{
  std::cout << "Log[" << log.lsn;
  std::cout << "," << log.offset ;
  std::cout << "]: trans_id: " << log.trans_id;
  std::cout << ", file_id: " << log.file_id;
  std::cout << ", type: " << log.type;
  std::cout << ", table_name: " << log.table_name;
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
Logger::setNumGroupCommit(int group_param)
{
  num_group_commit = group_param;
}

uint64_t
Logger::getTotalWriteSize()
{
  uint64_t total = 0;
  for(int i=0; i<MAX_WORKER_THREAD;i++){
    total += logBuffer[i].total_write_size;
  }

  return total;
}

uint32_t Logger::num_group_commit = 1;
