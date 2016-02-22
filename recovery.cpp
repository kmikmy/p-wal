#include "include/ARIES.h"
#include "include/dpt.h"
#include <mutex>
#include <cstdlib>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <iostream>
#include <exception>

using namespace std;

// recovery processing 用 トランザクションテーブル
TransTable recovery_trans_table;

extern DirtyPageTable dirty_page_table;
extern BufferControlBlock page_table[PAGE_N];
extern char* ARIES_HOME;
extern MasterRecord master_record;

extern void page_fix(int page_id, int th_id);
extern void page_unfix(int page_id);
extern void rollback(uint32_t xid);
static void page_table_debug();

static int log_fd;

#ifdef FIO
#define ALOGBUF_SIZE 128
class AnaLogBuffer{
private:
  int th_id; // 書き込みスレッドの番号に対応したid
  int log_fd; // logファイルへのFD
  LogHeader header; // LogHeader
  off_t base_addr; // LogHeaderの先頭アドレス
  off_t log_ptr; // 現在読んでいるlogの部分のアドレス
  uint32_t rest_nlog; // 残りのログの個数

  Log logs[ALOGBUF_SIZE]; // Analysis Log Buffer
  int idx; // アナリシスログバッファで現在のログポインタ
  int nlog; // LogBuffer内のログの個数


public:
  AnaLogBuffer(){
    clear();
    log_fd = open(Logger::logpath, O_RDONLY | O_SYNC);
  }

  /* AnaLogBufferを使用する前に、ログ(スレッド)番号を与えて一度呼び出す */
  void
  init(int _th_id)
  {
    th_id = _th_id;
    base_addr = (off_t)th_id * LOG_OFFSET;
    log_ptr = base_addr + sizeof(LogHeader);

    lseek(log_fd, base_addr, SEEK_SET);
    int ret = read(log_fd, &header, sizeof(LogHeader));
    if(-1 == ret){
      perror("read"); exit(1);
    }
    rest_nlog = header.count;

    readLogs();
  }

  void
  seek(uint64_t offset){
    uint32_t nth = (offset - (base_addr+sizeof(LogHeader))) / sizeof(Log); // (Log)*offset is (n+1)th log from head

    /* the next next() calls readLogs() */
    nlog = 0;
    idx = 0;

    if (nth >= header.count){
      rest_nlog = 0;
      log_ptr = base_addr+sizeof(LogHeader)+sizeof(Log)*header.count;
      return; // the next next() throws exception.
    }

    rest_nlog = header.count - nth;
    log_ptr = offset;
  }

  void
  term(){
    close(log_fd);
  }

  void
  clear(){
    idx=0;
    memset(logs,0,sizeof(logs));
  }

  int
  readLogs(){

    nlog = (rest_nlog<ALOGBUF_SIZE)?rest_nlog:ALOGBUF_SIZE; //読み込むログの個数を決める
    //    cout << "readLogs: " << nlog << endl;
    if(nlog <= 0){
      return -1;
    }

    clear();

    lseek(log_fd, log_ptr, SEEK_SET);
    int ret = read(log_fd, logs, sizeof(Log)*nlog);
    if(-1 == ret){
      perror("read"); exit(1);
    }
    log_ptr += ret;
    rest_nlog -= nlog;

    //    cout << "rest: " << rest_nlog << endl;

    return nlog;
  }

  //Logを一つ取り出す(ポインタを先へ進める)
  Log
  next()
  {
    // Logの再読み込み
    if(idx == nlog){
      if(readLogs()==-1)
	throw "read no log";
    }

    idx++;
    return logs[idx-1];
  }

  // 先頭のログを読む(ポインタは先へ進めない)
  Log
  front()
  {
    // Logの再読み込み
    if(idx == nlog){
      if(readLogs()==-1)
	throw "read no log";
    }

    return logs[idx];
  }
};
#endif

void
remove_transaction_xid(uint32_t xid){
  // 現状、recoverは逐次で行うため衝突が発生しないのでコメントアウト
  //  std::lock_guard<std::mutex> lock(trans_table_mutex);
  recovery_trans_table.erase(recovery_trans_table.find(xid));
}

static void
transaction_table_debug(){
  TransTable::iterator it;
  cout << "**************** Transaction Table ****************" << endl;
  for(it=recovery_trans_table.begin(); it!=recovery_trans_table.end(); ++it){
    std::cout << it->first << std::endl;
  }
  cout << endl;
}

static void
dirty_page_table_debug(){
  DirtyPageTable::iterator it;
  cout << "**************** Dirty Page Table ****************" << endl;
  for(it=dirty_page_table.begin(); it!=dirty_page_table.end(); it++){
    cout << " * " << (*it).page_id << ": LSN=" << (*it).rec_LSN << ", file_id="<< (*it).log_file_id << ", offset=" << (*it).rec_offset << endl;
  }
  cout << endl;
}


/* ログファイル毎のredo開始地点を求める. */
static void
min_recOffsets(uint64_t *min_offsets){
  for(int i=0; i<MAX_WORKER_THREAD; i++){
    min_offsets[i]=0;
  }

  DirtyPageTable::iterator it = dirty_page_table.begin();
  for(; it!=dirty_page_table.end(); it++){
    if(min_offsets[it->log_file_id] == 0){
      min_offsets[it->log_file_id] = it->rec_offset;
    }
    else if (min_offsets[it->log_file_id] > it->rec_offset) {
      cout << it->log_file_id << "'s min change into " << it->rec_LSN << endl;
      min_offsets[it->log_file_id] = it->rec_offset;
    }
  }
}


#ifndef FIO
static int
next_log(int log_fd, Log *log){
    int ret = read(log_fd, log, sizeof(Log));
    if(ret == -1){
      PERR("read");
    }

    return ret;
}
#else
static int
next_log(AnaLogBuffer *alogs, std::set<int> *exist_flags, Log *log){
  int min_id=-1;
  Log min_log,tmp_log;

  std::set<int>::iterator it;
  std::set<int> del_list;

  min_log.LSN = ~(uint64_t)0; // NOTのビット演算(uint64_tの最大値を求めている)
  for( it=(*exist_flags).begin(); it!=(*exist_flags).end(); it++){
    try{
      //	cout << *it << endl;
      tmp_log = alogs[*it].front();
      if(min_log.LSN > tmp_log.LSN){
	min_id = *it;
	min_log = tmp_log;
      }
    }
    catch(char const* e) {
      del_list.insert(*it);
      continue;
    }
  }

  for( it=del_list.begin(); it!=del_list.end(); it++){
    //      cout << "el: " << *it << endl;
    (*exist_flags).erase((*exist_flags).find(*it));
  }
  del_list.clear();

  if((*exist_flags).empty()){ // reached end of all log file.
    return 0;
  }

  if(min_id == -1){
    PERR("not found next processing log");
  }
  alogs[min_id].next(); // LSNが一番小さなログを持っているalogを一つ進める
  *log = min_log;

  return 1;
}
#endif

static void
analysis(uint64_t* redo_offsets){
  Log log;

#ifndef FIO
  LogSegmentHeader lh;
  lseek(log_fd, 0, SEEK_SET);
  if( -1 == read(log_fd, &lh, sizeof(LogHeader))){
    perror("read"); exit(1);
  }

  for(uint32_t i=0;i<lh.count;i++){
    int ret = next_log(log_fd, &log);
    if(ret == 0) break;

#else
  std::set<int> exist_flags;
  AnaLogBuffer alogs[MAX_WORKER_THREAD];

  for(int i=0;i<MAX_WORKER_THREAD;i++){
    alogs[i].init(i);
    exist_flags.insert(i);
  }

  while(1){
    int ret = next_log(alogs, &exist_flags, &log);
    if(ret == 0) break;

    // マスターレコードのLSNの復元
    ARIES_SYSTEM::master_record.system_last_lsn = log.LSN;
    Logger::log_debug(log);
#endif

    if(log.Type == BEGIN){
      //      cout << "Detect BEGIN for " << log.TransID << endl;;

      Transaction trans;
      trans.TransID = log.TransID;
      trans.State=U;
      trans.LastLSN=log.LSN;
      trans.LastOffset=log.Offset;
      trans.UndoNxtLSN=0;
      trans.UndoNxtOffset=0;

      recovery_trans_table[trans.TransID] = trans;
      if(ARIES_SYSTEM::master_record.system_xid < trans.TransID)
	ARIES_SYSTEM::master_record.system_xid = trans.TransID;
    }
    else if(log.Type == UPDATE || log.Type == COMPENSATION){
      recovery_trans_table[log.TransID].LastLSN = log.LSN;
      recovery_trans_table[log.TransID].LastOffset = log.Offset;

      if(log.Type == UPDATE){
	// if(log is undoable)
	recovery_trans_table[log.TransID].UndoNxtLSN = log.LSN;
	recovery_trans_table[log.TransID].UndoNxtOffset = log.Offset;
      }
      else { // log.Type == COMPENSATION
	recovery_trans_table[log.TransID].UndoNxtLSN = log.UndoNxtLSN;
	recovery_trans_table[log.TransID].UndoNxtOffset = log.UndoNxtOffset;
      }
#ifdef DEBUG
      cout << "log.PageID: " << log.PageID << endl;
#endif
      if(!dirty_page_table.contains(log.PageID)){ // まだdirty_page_tableにエントリがない
#ifdef DEBUG
	cout << "Added page_id: " << log.PageID << " in D.P.T" << endl;
#endif
	dirty_page_table.add(log.PageID, log.LSN, log.Offset, log.file_id);
      }

    } // if(log.Type == UPDATE || log.Type == COMPENSATION){
    else if(log.Type == END){
      remove_transaction_xid(log.TransID);
    }
  }

  std::list<uint32_t> del_list;
  for(TransTable::iterator it=recovery_trans_table.begin(); it!=recovery_trans_table.end(); it++){
    if(it->second.UndoNxtLSN == 0){
      // 「BEGINだけ書かれて終了したトランザクション」、または「rollbackが完了しているがENDログが書かれていないトランザクション」のENDログを書いて、トランザクションテーブルのエントリを削除する
      del_list.push_back(it->first);
    }
  }
  for(std::list<uint32_t>::iterator it=del_list.begin(); it!=del_list.end(); it++){
    Log end_log;
    memset(&end_log,0,sizeof(Log));
    end_log.Type = END;
    end_log.TransID = *it;
    // clog.before isn't needed because compensation log record is redo-only.
    end_log.UndoNxtLSN = 0; // PrevLSN of BEGIN record must be 0.
    end_log.UndoNxtOffset = 0;
    end_log.PrevLSN = recovery_trans_table.at(*it).LastLSN;
    end_log.PrevOffset = recovery_trans_table.at(*it).LastOffset;

    Logger::log_write(&end_log, 0);
#ifdef DEBUG
    Logger::log_debug(end_log);
#endif

    remove_transaction_xid(*it);
  }

#ifdef FIO
  for(int i=0;i<MAX_WORKER_THREAD;i++){
    alogs[i].term();
  }
#endif

  min_recOffsets(redo_offsets);
}

static void
redo(uint64_t *redo_offsets){
  Log log;

#ifndef FIO
  LogHeader lh;
  lseek(log_fd, 0, SEEK_SET);
  if( -1 == read(log_fd, &lh, sizeof(LogHeader))){
    perror("read"); exit(1);
  }

  lseek(log_fd, redo_offsets[0], SEEK_SET);
  for(uint32_t i=(uint32_t)(redo_offsets[0]-sizeof(LogHeader))/sizeof(Log);i<lh.count;i++){


    int ret = next_log(log_fd, &log);
    if(ret == 0){
      break;
    }

#else

  std::set<int> exist_flags;
  AnaLogBuffer alogs[MAX_WORKER_THREAD];

  for(int i=0;i<MAX_WORKER_THREAD;i++){
    alogs[i].init(i);
    alogs[i].seek(redo_offsets[i]);
    exist_flags.insert(i);
  }

  while(1){
    int ret = next_log(alogs, &exist_flags, &log);
    if(ret == 0) break;

#endif

    Logger::log_debug(log);

    if (log.Type == UPDATE || log.Type == COMPENSATION){
      int idx=log.PageID;
      if(dirty_page_table.contains(idx) && log.LSN >= dirty_page_table[idx].rec_LSN){
	// redoは並列に行わないのでページへのlockはいらない
	page_fix(idx, 0);

	if(page_table[idx].page.page_LSN < log.LSN){
	  page_table[idx].page.value = log.after;
	  page_table[idx].page.page_LSN = log.LSN;
	}
	else{ /* update dirty page list with correct info. this will happen if this
		 page was written to disk after the checkpt but before sys failure. */
	  dirty_page_table[idx].rec_LSN = page_table[idx].page.page_LSN + 1;
	}

        page_unfix(idx);
      }
    }
  }

#ifdef FIO
  for(int i=0;i<MAX_WORKER_THREAD;i++){
    alogs[i].term();
  }
#endif

#ifdef DEBUG
  page_table_debug();
#endif
}

/*
  [caution]: recoveryではLSNの逆順にundoする必要があるので、このrollbackはrecoveryで使えない。

  rollback_for_recovery()内ではトランザクションテーブルからエントリを削除しない。
  Iteratorを使って、トランザクションテーブルを巡回している場合があるため。
*/
void
rollback_for_recovery(uint32_t xid){
  uint64_t lsn = recovery_trans_table.at(xid).LastLSN; // rollbackするトランザクションの最後のLSN

  Log log;
  while(lsn != 0){ // lsnが0になるのはprevLSNが0のBEGINログを処理した後
    lseek(log_fd, lsn, SEEK_SET);

    int ret = read(log_fd, &log, sizeof(Log));
    if(ret == -1){
      perror("read"); exit(1);
    }

    if(ret == 0){
      cout << "illegal read" << endl;
      exit(1);
    }

#ifdef DEBUG
    Logger::log_debug(log);
#endif

    if (log.Type == UPDATE){
      int idx=log.PageID;

      page_table[idx].page.value = log.before;
      // 前のログレコードのLSNをページバッファに適用する
      Log tmp;
      lseek(log_fd, log.PrevLSN, SEEK_SET);
      if(read(log_fd, &tmp, sizeof(Log)) == -1) perror("read: log through PrevLSN");
      page_table[idx].page.page_LSN = tmp.LSN;

      Log clog;
      memset(&clog,0,sizeof(Log));
      clog.Type = COMPENSATION;
      clog.TransID = log.TransID;

      clog.PageID = log.PageID;
      clog.UndoNxtLSN = log.PrevLSN;
      clog.UndoNxtOffset = log.PrevOffset;
      // clog.before isn't needed because compensation log record is redo-only.
      clog.after = log.before;
      clog.PrevLSN = recovery_trans_table.at(xid).LastLSN;
      clog.PrevOffset = recovery_trans_table.at(xid).LastLSN;


      // compensation log recordをどこに書くかという問題はひとまず置いておく
      // とりあえず全部id=0のログブロックに書く
      ret = Logger::log_write(&clog, 0);
      recovery_trans_table[xid].LastLSN = clog.Offset;

#ifdef DEBUG
      Logger::log_debug(clog);
#endif

    }
    else if(log.Type == COMPENSATION){
      lsn = log.UndoNxtLSN;
      continue;
    }
    else if(log.Type == BEGIN){
      Log clog;
      memset(&clog,0,sizeof(Log));
      clog.Type = END;
      clog.TransID = log.TransID;
      // clog.before isn't needed because compensation log record is redo-only.
      clog.UndoNxtLSN = log.PrevLSN; // PrevLSN of BEGIN record must be 0.
      clog.PrevLSN = recovery_trans_table.at(xid).LastLSN;

      Logger::log_write(&clog, 0);
#ifdef DEBUG
      Logger::log_debug(clog);
#endif
    }
    lsn = log.PrevLSN;
  }

  recovery_trans_table.erase(recovery_trans_table.find(xid));

  //  close(log_fd);
}

/* 次にundoすべき最大のLSN値を持つログの"オフセット"を返す(LSNではない). */
static uint64_t
max_undo_nxt_offset(){
  uint64_t offset_max = 0;
  uint64_t LSN_max = 0;
  TransTable::iterator it;
  for(it=recovery_trans_table.begin(); it!=recovery_trans_table.end();it++){
    //    cout << "it->first=" << it->first << ", it->second.UndoNxtLSN=" << it->second.UndoNxtLSN << endl;

    if(it->second.UndoNxtLSN > LSN_max){
      LSN_max = it->second.UndoNxtLSN;
      offset_max = it->second.UndoNxtOffset;
    }
  }

  return offset_max;
}

static void
undo(){
  LogHeader lh;
  if( -1 == read(log_fd, &lh, sizeof(LogHeader))){
    perror("read"); exit(1);
  }

  // TransTable::iterator it;
  // for(it=recovery_trans_table.begin(); it!=recovery_trans_table.end();it++){
  //   rollback_for_recovery(it->second.TransID);
  // }

  Log log;
  while(!recovery_trans_table.empty()){ // 現在の実装ではトランザクションテーブルに残っているトランザクションエントリの状態は全て'U'
    uint64_t undo_nxt_offset = max_undo_nxt_offset();
    if(undo_nxt_offset == 0){
      PERR("undo_nxt_offset is 0");
    }

    cout << "undo_nxt_offset="  << undo_nxt_offset << endl;

    lseek(log_fd, undo_nxt_offset, SEEK_SET);
    int ret = read(log_fd, &log, sizeof(Log));
    if(ret == -1){
      PERR("read");
    } else if(ret == 0){
      PERR("log doesn't exist");
    }

#ifdef DEBUG
    Logger::log_debug(log);
#endif

    if (log.Type == UPDATE){
      int idx=log.PageID;

      page_table[idx].page.value = log.before;

      Log clog;
      memset(&clog,0,sizeof(Log));
      clog.Type = COMPENSATION;
      clog.TransID = log.TransID;
      clog.PageID = log.PageID;
      clog.UndoNxtLSN = log.PrevLSN;
      clog.UndoNxtOffset = log.PrevOffset;
      /* clog.before isn't needed to store "before value" because compensation log record is redo-only. but, it is stored for visibility. */
      clog.before = log.after;
      clog.after = log.before;
      clog.PrevLSN = recovery_trans_table[log.TransID].LastLSN;
      clog.PrevOffset = recovery_trans_table[log.TransID].LastOffset;

      // compensation log recordをどこに書くかという問題はひとまず置いておく
      // とりあえず全部id=0のログブロックに書く
      ret = Logger::log_write(&clog, 0);

      page_table[idx].page.page_LSN = clog.LSN;

      recovery_trans_table[log.TransID].LastLSN = clog.LSN;
      recovery_trans_table[log.TransID].LastOffset = clog.Offset;

      recovery_trans_table[log.TransID].UndoNxtLSN = log.PrevLSN;
      recovery_trans_table[log.TransID].UndoNxtOffset = log.PrevOffset;

#ifdef DEBUG
      Logger::log_debug(clog);
#endif

    }
    else if(log.Type == COMPENSATION){
      recovery_trans_table[log.TransID].UndoNxtLSN = log.UndoNxtLSN;
      recovery_trans_table[log.TransID].UndoNxtOffset = log.UndoNxtOffset;
    }
    else if(log.Type == BEGIN){
      Log clog;
      memset(&clog,0,sizeof(Log));
      clog.Type = END;
      clog.TransID = log.TransID;
      clog.UndoNxtLSN = log.PrevLSN; // PrevLSN of BEGIN record must be 0.
      clog.UndoNxtLSN = log.PrevOffset;
      clog.PrevLSN = recovery_trans_table[log.TransID].LastLSN;
      clog.PrevOffset = recovery_trans_table[log.TransID].LastOffset;

      Logger::log_write(&clog, 0);
#ifdef DEBUG
      Logger::log_debug(clog);
#endif

      recovery_trans_table.erase(recovery_trans_table.find(log.TransID));
    }
  }

  recovery_trans_table.clear();
}


static void
page_table_debug(){
  cout << "**************** Page Table ****************" << endl;
  for(int i=0;i<PAGE_N;i++){
    if(page_table[i].page.page_LSN == 0) continue;
    cout << "page[" << page_table[i].page.pageID << "]: page_LSN=" << page_table[i].page.page_LSN << ", value=" << page_table[i].page.value << endl;
  }
  cout << endl;
}

void recovery(){
  log_fd = open(Logger::logpath, O_CREAT | O_RDONLY);
  if(log_fd == -1){
    perror("open"); exit(1);
  }
  uint64_t *redo_offsets = (uint64_t *)malloc(sizeof(uint64_t)*MAX_WORKER_THREAD);
  if(redo_offsets == NULL) PERR("redo_lsn == NULL");
  analysis(redo_offsets);
  cout << "done through analysis pass." << endl;
  transaction_table_debug();
  dirty_page_table_debug();

  cout << "**************** redo_offsets ****************" << endl;
  for(int i=0;i<MAX_WORKER_THREAD;i++){
    if(redo_offsets[i] == 0) continue;
    cout << i << ": " << redo_offsets[i] << endl;
  }
  cout << endl;

  redo(redo_offsets);
  free(redo_offsets);
  cout << "done through redo pass." << endl;
  page_table_debug();

  undo();
  cout << "done through undo pass." << endl;
  page_table_debug();

  Logger::log_all_flush();

  close(log_fd);
}
