#include "include/ARIES.h"
#include "include/log.h"
#include <iostream>
#include <utility>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <mutex>
#include <cstring>
#include <fstream>

#define _NVM

#ifdef _NVM
#include <nvm/nvm_primitives.h>
#include <nvm/nvm_types.h>
#endif


using namespace std;

// #define DEBUG

/*
   Group Commit の際のパラメータ. Defaultは1.
   Logger::set_num_group_commit()でセットする.
*/

typedef struct WriteData {
  struct timeval t;
  uint32_t s;
} WriteData;

const size_t MAX_LOG_RECORD_SIZE = 4096;

#ifdef FIO
const char* Logger::logpath = "/dev/fioa";
//const char* Logger::logpath = "/dev/sdc1";
#else
const char* Logger::logpath = "/dev/fioa";
//const char* Logger::logpath = "/work2/tmp/log.dat";
//const char* Logger::logpath = "/dev/sdc1";
#endif

double time_of_logging[MAX_WORKER_THREAD];

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

static double
getDiffTimeSec(struct timeval begin, struct timeval end){
  double Diff = (end.tv_sec*1000*1000+end.tv_usec) - (begin.tv_sec*1000*1000+begin.tv_usec);
  return Diff / 1000. / 1000. ;
}

/*
   LogBuffer:

   まとめてwriteする必要があるので予めLogの配列を用意しておく。
   現在何個Logを所持しているかを管理する必要がある。
*/
class LogBuffer{
 private:
  static const unsigned MAX_CHUNK_LOG_SIZE=16*1024*1024;
  unsigned ptr_on_chunk_;
  int log_fd;
  off_t segment_base_addr;
  int th_id;
  int buffer_id_; // this flag's value is 0 or 1.

#ifdef _NVM
  nvm_handle_t handle = -1;
  nvm_iovec_t *iov = NULL;
  size_t nvm_max_iov_len_size = 0;
  size_t nvm_sector_size = 0;
#endif

#ifndef FIO
  volatile size_t next_release_lsn = 0; // for decoupling buffer allocation for Aether
#endif

 public:
  ChunkLogHeader chunk_log_header[2];
  uint64_t total_write_size;
  vector<struct WriteData> write_call_times;


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
    write_call_times.reserve(1000);
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

    nvm_capability_t nvm_capability[3];
    nvm_capability[0].cap_id = NVM_CAP_ATOMIC_WRITE_MULTIPLICITY_ID;
    nvm_capability[1].cap_id = NVM_CAP_ATOMIC_WRITE_MAX_VECTOR_SIZE_ID;
    nvm_capability[2].cap_id = NVM_CAP_SECTOR_SIZE_ID;

    int ret = nvm_get_capabilities(handle, nvm_capability, 3, false);
    if(ret < 3){ std::cerr << ret << std::endl; PERR("nvm_get_capabilities"); }
    nvm_max_iov_len_size = nvm_capability[0].cap_value * nvm_capability[1].cap_value;
    nvm_sector_size = nvm_capability[2].cap_value;
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
    iov = (nvm_iovec_t *) malloc(sizeof(nvm_iovec_t) * 16);
    if (iov == NULL){
      PERR("malloc");
    }


    //    iov[0].iov_base is decided whether log_segment_header[0,1] when write()
    iov[0].iov_len    = 512;
    iov[0].iov_opcode = NVM_IOV_WRITE;
    iov[0].iov_lba    = segment_base_addr / nvm_sector_size;

    for(int i=1;i<16;++i){
      // iov[i].iov_vase, iov_len, iov_lba are decided when write()
      iov[i].iov_opcode = NVM_IOV_WRITE;
    }
#endif

    /* 初めに使うチャンクログ領域を初期化する */
    clearLog(0);

    lseek(log_fd, segment_base_addr, SEEK_SET);
    int ret = read(log_fd, log_segment_header[0], sizeof(LogSegmentHeader));
    if(-1 == ret){
	cout << "log file read error" << endl;
	perror("read"); exit(1);
    }

#ifndef FIO
    next_release_lsn = nextOffset();
#endif
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

#ifndef FIO
    mtx_for_write.lock();
#endif

    /* チャンクを切り替え */
    toggleChunk();

#ifndef FIO
    mtx_for_insert.unlock();
#endif
    /* この時点で、切り替わったチャンクにログを挿入可能 */

    nolockFlush(buffer_id);
#ifndef FIO
    mtx_for_write.unlock();
#endif
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
#ifndef FIO
    next_release_lsn = nextOffset();
#endif
  }

  /*
    LogSegmentHeaderはsegment_base_addrに位置し、複数のChunkLogがその後に続く
   */
  void
  nolockFlush(int _select_buffer)
  {
    int id = _select_buffer;
    uint64_t chunk_head_pos = log_segment_header[id]->segment_size - chunk_log_header[id].chunk_size;
    // struct timeval io_time;
    // struct WriteData wd;

    /* チャンクの先頭にヘッダを記録 */
    memcpy( log_buffer_body[id], &chunk_log_header[id], sizeof(ChunkLogHeader));

    /* チャンクログの書き込み開始地点にlseek */
    off_t write_pos = segment_base_addr + chunk_head_pos;

#ifndef _NVM
    int ret;
    lseek(log_fd, write_pos, SEEK_SET);
    if( -1 == (ret = write( log_fd, log_buffer_body[id], chunk_log_header[id].chunk_size)) ){
      cerr << "log_fd: " << log_fd << ",log_buffer_body[id]" << (void *)log_buffer_body[id] << ", chunk_log_header[id].chunk_size" << chunk_log_header[id].chunk_size << endl;
       perror("write(ChunkLog)");
       exit(1);
    }

    // gettimeofday(&io_time,NULL);
    // wd.t = io_time;
    // wd.s = ret;
    // write_call_times.push_back(wd);

    /* ログセグメントの先頭(ログセグメントヘッダの買い込み開始地点)にlseek */
    lseek(log_fd, segment_base_addr, SEEK_SET);
    if(-1 == (ret = write(log_fd, log_segment_header[id], sizeof(LogSegmentHeader))) ){
       perror("write(LogSegmentHeader)"); exit(1);
    }

    // gettimeofday(&io_time,NULL);
    // wd.t = io_time;
    // wd.s = ret;
    // write_call_times.push_back(wd);

    fsync(log_fd);
#else
    iov[0].iov_base   = (uint64_t)((uintptr_t) log_segment_header[id]);

    int64_t remain = chunk_log_header[id].chunk_size;
    int iov_cnt = 1;
    while(remain > 0){
      iov[iov_cnt].iov_base   = (uint64_t)((uintptr_t) log_buffer_body[id]) + iov_cnt * nvm_max_iov_len_size;
      iov[iov_cnt].iov_len    = remain <= (int64_t)nvm_max_iov_len_size ? remain : nvm_max_iov_len_size;
      iov[iov_cnt].iov_lba    = write_pos / nvm_sector_size + (iov_cnt * nvm_max_iov_len_size / nvm_sector_size );
      ++iov_cnt;
      remain -= nvm_max_iov_len_size;
    }

    /**************************************/
    /* perform the batch atomic operation */
    /**************************************/
    int bytes_written = 0;
    bytes_written = nvm_batch_atomic_operations(handle, iov, iov_cnt, 0);
    //    std::cerr << bytes_written << std::endl;

    if ( bytes_written < 0 || bytes_written != (int)(chunk_log_header[id].chunk_size + nvm_sector_size) ){
      std::cerr << "bytes_written: " << bytes_written << std::endl;
      std::cerr <<  "handle: " << handle << ",iov_base: " << iov[0].iov_base << ", iov_len: " << iov[0].iov_len << ", iov_lba:" << iov[0].iov_lba << std::endl;
      PERR("nvm_batch_atomic_operations");
    }

    // gettimeofday(&io_time,NULL);
    // wd.t = io_time;
    // wd.s = bytes_written;
    // write_call_times.push_back(wd);

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
  push(char *data, size_t size, size_t insertPtr)
  {
    Log *log = (Log *)data;
    int id = getDoubleBufferFlag();
    if(ptr_on_chunk_ + size >= MAX_CHUNK_LOG_SIZE){
      perror("can't push");
      exit(1);
    }

    // Logger::logDebug(*log);
    memcpy(&log_buffer_body[id][insertPtr], data, size);

#ifndef FIO
    //    std::cout << "log->lsn: " << log->lsn << ", next_release_lsn: " << next_release_lsn << endl;
    while(next_release_lsn != log->lsn){;
      //      std::cout << "log->lsn: " << log->lsn << ", next_release_lsn: " << next_release_lsn << endl;
    }
    next_release_lsn = log->lsn + size;
#endif
    //    std::cout << "ptr: " << ptr_on_chunk_ << std::endl;
  }

  /* この関数を呼び出す前にインサートロックを獲得している */
  /* WALバッファ上の挿入位置を返す */
  size_t
  bufferAcquire(char *data)
  {
    Log *log = (Log *)data;
    LSN_and_Offset _ret;
    off_t _pos = nextOffset();
    int id = getDoubleBufferFlag();
    size_t size = log->total_length;
    size_t insertPtr;

    chunk_log_header[id].log_record_num++;

    insertPtr = ptr_on_chunk_;
    ptr_on_chunk_ += size;

    log->offset = _pos;
    log->file_id = th_id;
#ifdef FIO
    log->lsn = __sync_fetch_and_add(&ARIES_SYSTEM::master_record.system_last_lsn, 1);
#else
    log->lsn = _pos; // Normal WALではLSNとoffsetは等しい
    ARIES_SYSTEM::master_record.system_last_lsn = _pos; // システムのGlobalLSNの更新
#endif

    bool try_push = true;
    if( log->type == END && num_commit == Logger::num_group_commit ){
      while(try_push){
	if(ableToAdd(log->total_length)){
	  push(data, log->total_length, insertPtr);
	  try_push = false;
	} else {
	  flush(); // flush()の中でinsertロックを開放している
#ifndef FIO
	  mtx_for_insert.lock(); // 再度insertロックの獲得を試みる
#endif
	}
	flush(); // flush()の中でinsertロックを開放している
      }
    } else { ;
#ifndef FIO
#ifdef AETHER
      mtx_for_insert.unlock();
#endif
#endif
    }

    return insertPtr;
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
  // struct timeval begin, end;

  LSN_and_Offset lsn_and_offset;
  bool try_push = true;
  char data[MAX_LOG_RECORD_SIZE];
  size_t ptr = 0;

  // gettimeofday(&begin, NULL);

  // log->total_length = 4096;
  memcpy(data, log, sizeof(Log));
  ptr += sizeof(Log);
  for(unsigned i=0; i<field_log_list.size(); i++){
    memcpy(&data[ptr], &field_log_list[i], sizeof(FieldLogHeader)); // 更新フィールドのオフセットと長さ
    ptr += sizeof(FieldLogHeader);
    memcpy(&data[ptr], &field_log_list[i].before, field_log_list[i].field_length);
    ptr += field_log_list[i].field_length;
    memcpy(&data[ptr], &field_log_list[i].after, field_log_list[i].field_length);
    ptr += field_log_list[i].field_length;
  }

  //  std::cerr << log->total_length << std::endl;

#ifndef FIO
  th_id = 0;
  logBuffer[th_id].mtx_for_insert.lock();
#endif

  bool flushed = false;
  if(log->type == END){
    logBuffer[th_id].num_commit++;
    if( logBuffer[th_id].num_commit == Logger::num_group_commit ){
      flushed = true;
    }
  }

  size_t insertPtr = 0;
  insertPtr = logBuffer[th_id].bufferAcquire(data);
  //  Logger::logDebug(*(Log *)data);

  if(flushed){
    return 0;
  }

  while(try_push){
    if(logBuffer[th_id].ableToAdd(log->total_length)){
      logBuffer[th_id].push(data, log->total_length, insertPtr); // memcpy
      try_push = false;
    } else {
      logBuffer[th_id].flush(); // flush()の中でinsertロックを開放している
#ifndef FIO
      logBuffer[th_id].mtx_for_insert.lock(); // 再度insertロックの獲得を試みる
#endif
    }
  }
    // gettimeofday(&end, NULL);
    // time_of_logging[th_id] += getDiffTimeSec(begin, end);
#ifndef FIO
#ifndef AETHER
  logBuffer[th_id].mtx_for_insert.unlock();
#endif
#endif

  return 0;
}

/* ログバッファのフラッシュを行う */
void
Logger::logFlush(int th_id)
{
#ifndef FIO
  th_id = 0;
#endif

#ifndef FIO
  logBuffer[th_id].mtx_for_insert.lock();
#endif
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
  std::cerr << "Log[" << log.lsn;
  std::cerr << "," << log.offset ;
  std::cerr << "]: trans_id: " << log.trans_id;
  std::cerr << ", file_id: " << log.file_id;
  std::cerr << ", type: " << log.type;
  std::cerr << ", table_name: " << log.table_name;
  std::cerr << ", page_id: " << log.page_id;
  std::cerr << ", prev_lsn: " << log.prev_lsn;
  std::cerr << ", undo_nxt_lsn: " << log.undo_nxt_lsn;
  if(log.type == UPDATE || log.type == COMPENSATION){
    // フィールドを表示する
    //    std::cerr << ", before: " << log.before;
    //    std::cerr << ", after: " << log.after;
    //    std::cerr << ", op.op_type: " << log.op.op_type;
    //    std::cerr << ", op.amount: " << log.op.amount ;
  }
  std::cerr << std::endl;
}

void
Logger::printAllWriteTimes()
{
  uint64_t base = logBuffer[0].write_call_times[0].t.tv_sec * 1000 * 1000 +
    logBuffer[0].write_call_times[0].t.tv_usec;

  string filename = "/tmp/write_call_times_all.txt";
  std::ofstream ofs1(filename);
  for(int i=0; i<MAX_WORKER_THREAD;i++){
      for(auto wd: logBuffer[i].write_call_times){
      ofs1 << (uint64_t)wd.t.tv_sec  * 1000 * 1000 + wd.t.tv_usec - base << ' ';
      ofs1 << wd.s << std::endl;
    }
  }

  filename = "/tmp/write_call_times_0.txt";
  std::ofstream ofs2(filename);
  for(auto wd: logBuffer[0].write_call_times){
      ofs2 << (uint64_t)wd.t.tv_sec  * 1000 * 1000 + wd.t.tv_usec - base << ' ';
      ofs2 << wd.s << std::endl;
  }

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
