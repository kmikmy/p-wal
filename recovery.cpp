#include "ARIES.h"
#include "dpt.h"
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
extern void rollback(uint32_t xid);
static void page_table_debug();


static int log_fd;

void 
remove_transaction_xid(uint32_t xid){
  // 現状、recoverは逐次で行うため衝突が発生しないのでコメントアウト
  //  std::lock_guard<std::mutex> lock(trans_table_mutex); 
  recovery_trans_table.erase(recovery_trans_table.find(xid));
}

static void
show_transaction_table(){
  map<uint32_t,Transaction>::iterator it;
  
  for(it=recovery_trans_table.begin(); it!=recovery_trans_table.end(); ++it){
    std::cout << it->first << std::endl;
  }
}


static uint32_t
min_recLSN(){
  DirtyPageTable::iterator it = dirty_page_table.begin();
  if(it == dirty_page_table.end()){
    return 0;
  }

  uint32_t min_LSN = (*it).rec_LSN;
#ifdef DEBUG
  cout << "page_id: " << (*it).page_id << ", LSN: " << (*it).rec_LSN << endl;
#endif

  for(it++; it!=dirty_page_table.end(); it++){
#ifdef DEBUG
    cout << "page_id: " << (*it).page_id << ", LSN: " << (*it).rec_LSN << endl;
#endif
    if(min_LSN > (*it).rec_LSN) min_LSN = (*it).rec_LSN;
  }

  return min_LSN;
}

static uint32_t
sequential_analysis(){ 
  LogHeader lh;
  lseek(log_fd, 0, SEEK_SET);
  if( -1 == read(log_fd, &lh, sizeof(LogHeader))){
    perror("read"); exit(1);
  }

  Log log;
  for(uint32_t i=0;i<lh.count;i++){
    int ret = read(log_fd, &log, sizeof(Log));  
    if(ret == -1){
      perror("read"); exit(1);
    }

    if(ret == 0)
      break;

    if(log.Type == BEGIN){
      //      cout << "Detect BEGIN for " << log.TransID << endl;;

      Transaction trans;
      trans.TransID = log.TransID;
      trans.State=U;
      trans.LastLSN=log.offset;
      trans.UndoNxtLSN=0;
      
      recovery_trans_table[trans.TransID] = trans;
      if(ARIES_SYSTEM::master_record.system_xid < trans.TransID)
	ARIES_SYSTEM::master_record.system_xid = trans.TransID;
    } 
    else if(log.Type == UPDATE || log.Type == COMPENSATION){
      recovery_trans_table[log.TransID].LastLSN = log.offset;

      if(log.Type == UPDATE){
	// if(log is undoable)
	recovery_trans_table[log.TransID].UndoNxtLSN = log.offset;
      } 
      else { // log.Type == COMPENSATION
	recovery_trans_table[log.TransID].UndoNxtLSN = log.UndoNxtLSN;
      } 
#ifdef DEBUG
      cout << "log.PageID: " << log.PageID << endl;
#endif
      if(!dirty_page_table.contains(log.PageID)){ // まだdirty_page_tableにエントリがない
#ifdef DEBUG
	cout << "Added page_id: " << log.PageID << " in D.P.T" << endl;
#endif
	dirty_page_table.add(log.PageID,log.LSN);
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
    end_log.PrevLSN = recovery_trans_table.at(*it).LastLSN; 
      
    Logger::log_write(&end_log, 0);
#ifdef DEBUG
    Logger::log_debug(end_log);
#endif

    remove_transaction_xid(*it); 
  }

  Logger::log_all_flush();
  cout << "seq analysis end" << endl;
  return min_recLSN();
}

#ifdef FIO
#define ALOGBUF_SIZE 1000

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
    cout << "readLogs: " << nlog << endl;
    if(nlog == 0){
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

    cout << "rest: " << rest_nlog << endl;
    cout << "nlog: " << nlog << endl;
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

static uint32_t
parallel_analysis(){
  std::set<int> flags;  
  AnaLogBuffer alogs[MAX_WORKER_THREAD];

  for(int i=0;i<MAX_WORKER_THREAD;i++){
    alogs[i].init(i);
    flags.insert(i);
  }
  
  int min_id;
  Log min_log,tmp_log;

  std::set<int>::iterator it;
  std::set<int> del_list;
  while(1){
    min_log.LSN = ~(uint32_t)0; // NOTのビット演算
    for( it=flags.begin(); it!=flags.end(); it++){
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
      flags.erase(flags.find(*it));
    }
    del_list.clear();

    if(flags.empty()){
      break;
    }


    // cout << "search log_file is: ";
    // for( it=flags.begin(); it!=flags.end(); it++){
    //   cout << *it ;
    // }
    // cout << endl;

    
    //　LSNが一番小さなログを処理する
    alogs[min_id].next();
    Log log = min_log;
    
    Logger::log_debug(log);
    //    sleep(1);
    if(log.Type == BEGIN){
      Transaction trans;
      trans.TransID = log.TransID;
      trans.State=U;
      trans.LastLSN=log.offset;
      trans.UndoNxtLSN=0;
      
      recovery_trans_table[trans.TransID]=trans;
    } 
    else if(log.Type == UPDATE){
      recovery_trans_table[log.TransID].LastLSN = log.LSN;
    } 
    else if(log.Type == COMPENSATION){
      recovery_trans_table[log.TransID].LastLSN = log.LSN;
      if(log.UndoNxtLSN == 0){ 
	// BEGINログをCOMPENSATIONしたのでトランザクションテーブルから削除する
	remove_transaction_xid(log.TransID); 
      }
    }
    else if(log.Type == END){
      remove_transaction_xid(log.TransID);
    }
  }

  for(int i=0;i<MAX_WORKER_THREAD;i++){
    alogs[i].term();
  }

  return 0;
}
#endif

// Transactionテーブルをログから復元する
static uint32_t
analysis(){
  uint32_t redo_lsn;
  cout << "analysis() start" << endl;

#ifndef FIO
  redo_lsn = sequential_analysis();
#else
  redo_lsn = parallel_analysis();
#endif 

  //  ARIES_SYSTEM::transtable_debug();
  cout << redo_lsn << endl;

  return redo_lsn;
}


static bool 
redo_test(){
  int fd;
  std::string page_filename = ARIES_HOME;
  page_filename += "/data/pages.dat";

  if( (fd = open(page_filename.c_str(),  O_RDONLY )) == -1){
    perror("open");
    exit(1);
  }

  lseek(fd,0,SEEK_SET);
  Page p;
  int ret; int n=0;
  while( 0 != (ret = read(fd, &p, sizeof(Page)))){
    if(ret == -1){
      perror("exit"); exit(1);
    }

    if( p.pageID != page_table[n].page.pageID || p.page_LSN != page_table[n].page.page_LSN || p.value != page_table[n].page.value ){
      return false;
    }
    n++;
  }

  return true;
}


static void
sequential_redo(){
  LogHeader lh;
  lseek(log_fd, 0, SEEK_SET);
  if( -1 == read(log_fd, &lh, sizeof(LogHeader))){
    perror("read"); exit(1);
  }
  
  Log log;
  for(uint32_t i=0;i<lh.count;i++){
    int ret = read(log_fd, &log, sizeof(Log));  
    if(ret == -1){
      perror("read"); exit(1);
    }
    if(ret == 0)
      break;

#ifdef DEBUG
    Logger::log_debug(log);
#endif

    if (log.Type == UPDATE || log.Type == COMPENSATION){
      int idx=log.PageID;
      
      // redoは並列に行わないのでページへのlockはいらない
      page_fix(idx, 0);
      
      if(page_table[idx].page.page_LSN <= log.LSN){
	page_table[idx].page.value = log.after;
	//	page_table[idx].page.page_LSN = log.LSN;  必要ない
      }
    }
  }

#ifdef DEBUG
  page_table_debug();
#endif

  if(redo_test()) 
    cout << "success redo test!" << endl << endl;
  else
    cout << "failed redo test! It possibly need undo!" << endl << endl;
}


static void
parallel_redo(){

}

static void 
redo(){
#ifndef FIO
  sequential_redo();
#else
  parallel_redo();
#endif 
  
}


/*
  rollback_for_recovery()内ではトランザクションテーブルからエントリを削除しない。
  Iteratorを使って、トランザクションテーブルを巡回している場合があるため。
*/
void
rollback_for_recovery(uint32_t xid){
  // int log_fd = open(Logger::logpath, O_RDONLY);
  // if(log_fd == -1){
  //   perror("open"); exit(1);
  // }
  
  uint32_t lsn = recovery_trans_table.at(xid).LastLSN; // rollbackするトランザクションの最後のLSN

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
      // clog.before isn't needed because compensation log record is redo-only.
      clog.after = log.before;
      clog.PrevLSN = recovery_trans_table.at(xid).LastLSN;


      // compensation log recordをどこに書くかという問題はひとまず置いておく
      // とりあえず全部id=0のログブロックに書く
      ret = Logger::log_write(&clog, 0); 
      recovery_trans_table[xid].LastLSN = clog.offset;

#ifdef DEBUG
      Logger::log_debug(clog);
#endif

    }
    else if(log.Type == COMPENSATION){
      lsn = log.UndoNxtLSN;
      continue;
    }
    else if(log.Type == BEGIN){
      /* BEGIN のCOMPENSATEは END にするべき(未実装) */
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


static void 
undo(){
  LogHeader lh;  
  if( -1 == read(log_fd, &lh, sizeof(LogHeader))){
    perror("read"); exit(1);
  }

  map<uint32_t, Transaction>::iterator it;
  for(it=recovery_trans_table.begin(); it!=recovery_trans_table.end();it++){
    rollback_for_recovery(it->second.TransID);    
  }

  recovery_trans_table.clear();

  //  page_undo_write(PAGE_N);

#ifdef DEBUG
  page_table_debug();
#endif
}

static void
page_table_debug(){
  cout << endl << "**************** Page Table ****************" << endl;
  for(int i=0;i<PAGE_N;i++){
    if(page_table[i].page.pageID != i) continue;
    cout << "page[" << page_table[i].page.pageID << "]: page_LSN=" << page_table[i].page.page_LSN << ", value=" << page_table[i].page.value << endl;
  cout << endl;
  }
}


/* (undoの後に呼び出される処理だが、実際は単純に)ページを書き出す処理 */
static void 
page_undo_write(unsigned n){
  int fd;
  std::string page_filename = ARIES_HOME;
  page_filename += "/data/pages.dat";

  if( (fd = open(page_filename.c_str(),  O_WRONLY )) == -1){
    perror("open");
    exit(1);
  }
  
  lseek(fd,0,SEEK_SET);

  for(unsigned i=0;i<n;i++){
    if( -1 == write(fd, &page_table[i].page, sizeof(Page))){
      perror("write"); exit(1);
    }    
  }  
}

void recovery(){
  log_fd = open(Logger::logpath, O_CREAT | O_RDONLY);
  if(log_fd == -1){
    perror("open"); exit(1);
  }
  uint32_t redo_lsn = analysis();
  redo();
  undo();

  close(log_fd);
}

