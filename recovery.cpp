#include "ARIES.h"
#include <mutex>
#include <cstdlib>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <iostream>
#include <exception>

using namespace std;

extern TransTable trans_table;
extern map<uint32_t, uint32_t> dirty_page_table;
extern BufferControlBlock page_table[PAGE_N];
extern char* ARIES_HOME;

extern void page_fix(int page_id, int th_id);
extern void append_transaction(Transaction trans);
extern void remove_transaction_xid(uint32_t xid);
extern void rollback(uint32_t xid);

static uint32_t
min_recLSN(){
  map<uint32_t, uint32_t>::iterator it = dirty_page_table.begin();
  if(it == dirty_page_table.end()){
    return 0;
  }
  
  uint32_t min_LSN = it->second;

  for(; it!=dirty_page_table.end(); it++){
    if(min_LSN > it->second) min_LSN = it->second;
  }
  return min_LSN;
}

static uint32_t
sequential_analysis(){ 
  int log_fd = open(Logger::logpath, O_CREAT | O_RDONLY);
  if(log_fd == -1){
    perror("open"); exit(1);
  }
  
  LogHeader header;
  lseek(log_fd, 0, SEEK_SET);
  if( -1 == read(log_fd, &header, sizeof(LogHeader))){
    perror("read"); exit(1);
  }

  Log log;
  while(1){
    int ret = read(log_fd, &log, sizeof(Log));  
    if(ret == -1){
      perror("read"); exit(1);
    }

    if(ret == 0)
      break;

    if(log.Type == BEGIN){
      Transaction trans;
      trans.TransID = log.TransID;
      trans.State=U;
      trans.LastLSN=log.LSN;
      trans.UndoNxtLSN=0;
      
      append_transaction(trans);
    } 
    else if(log.Type == UPDATE || log.Type == COMPENSATION){
      trans_table[log.TransID].LastLSN = log.LSN;

      if(log.Type == UPDATE){
	// if(log is undoable)
	trans_table[log.TransID].UndoNxtLSN = log.LSN;
      } 
      else { // log.Type == COMPENSATION
	trans_table[log.TransID].UndoNxtLSN = log.UndoNxtLSN;
      
	if(log.UndoNxtLSN == 0){ 
	  // BEGINログをCOMPENSATIONしたのでトランザクションテーブルから削除する
	  remove_transaction_xid(log.TransID); 
	}

	if(dirty_page_table.find(log.PageID) == dirty_page_table.end()){
	  dirty_page_table[log.PageID] = log.LSN;
	}
      } 
    } // if(log.Type == UPDATE || log.Type == COMPENSATION){
    else if(log.Type == END){
      remove_transaction_xid(log.TransID);
    }
  }
  close(log_fd);

  return min_recLSN();
}


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
      trans.LastLSN=log.LSN;
      trans.UndoNxtLSN=0;
      
      append_transaction(trans);
    } 
    else if(log.Type == UPDATE){
      trans_table[log.TransID].LastLSN = log.LSN;
    } 
    else if(log.Type == COMPENSATION){
      trans_table[log.TransID].LastLSN = log.LSN;
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

// Transactionテーブルをログから復元する
static uint32_t
analysis(){
  uint32_t redo_lsn;
#ifndef FIO
  redo_lsn = sequential_analysis();
#else
  redo_lsn = parallel_analysis();
#endif 

  ARIES_SYSTEM::transtable_debug();
  cout << redo_lsn << endl;

  sleep(10);

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
  int log_fd = open(Logger::logpath, O_RDONLY);
  if(log_fd == -1){
    perror("open"); exit(1);
  }

  LogHeader lh;
  lseek(log_fd, 0, SEEK_SET);
  if( -1 == read(log_fd, &lh, sizeof(LogHeader))){
    perror("read"); exit(1);
  }
  
  Log log;
  while(1){
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
  close(log_fd);

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
  int log_fd = open(Logger::logpath, O_RDONLY);
  if(log_fd == -1){
    perror("open"); exit(1);
  }
  
  uint32_t lsn = trans_table.at(xid).LastLSN; // rollbackするトランザクションの最後のLSN

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
      page_table[idx].page.page_LSN = log.PrevLSN; 

      Log clog;
      memset(&clog,0,sizeof(Log));
      clog.Type = COMPENSATION;
      clog.TransID = log.TransID;

      clog.PageID = log.PageID;
      clog.UndoNxtLSN = log.PrevLSN;
      // clog.before isn't needed because compensation log record is redo-only.
      clog.after = log.before;
      clog.PrevLSN = trans_table.at(xid).LastLSN;


      // compensation log recordをどこに書くかという問題はひとまず置いておく
      // とりあえず全部id=0のログブロックに書く
      ret = Logger::log_write(&clog, 0); 
      trans_table[xid].LastLSN = clog.LSN;

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
      clog.Type = COMPENSATION;
      clog.TransID = log.TransID;
      // clog.before isn't needed because compensation log record is redo-only.
      clog.UndoNxtLSN = log.PrevLSN;
      clog.PrevLSN = trans_table.at(xid).LastLSN;

      Logger::log_write(&clog, 0);
      // compensation logはcompensateされないのでtransaction tableに記録する必要はない
      trans_table[xid].LastLSN = clog.LSN;
#ifdef DEBUG
      Logger::log_debug(clog);
#endif
      
    }
    lsn = log.PrevLSN;
  }

  close(log_fd);
}


static void 
undo(){
  int log_fd = open(Logger::logpath, O_RDONLY);
  if(log_fd == -1){
    perror("open"); exit(1);
  }

  LogHeader lh;  
  if( -1 == read(log_fd, &lh, sizeof(LogHeader))){
    perror("read"); exit(1);
  }

  map<uint32_t, Transaction>::iterator it;
  for(it=trans_table.begin(); it!=trans_table.end();it++){
    rollback_for_recovery(it->second.TransID);    
  }

  trans_table.clear();

  //  page_undo_write(PAGE_N);

#ifdef DEBUG
  page_table_debug();
#endif

  close(log_fd);
  
}

static void
page_table_debug(){
  cout << endl << "**************** Page Table ****************" << endl;
  for(int i=0;i<PAGE_N;i++)
    cout << "page[" << page_table[i].page.pageID << "]: page_LSN=" << page_table[i].page.page_LSN << ", value=" << page_table[i].page.value << endl;
  cout << endl;
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
  
  uint32_t redo_lsn = analysis();
  redo();
  undo();
}

