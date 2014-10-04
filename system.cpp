#include "ARIES.h"
#include <mutex>
#include <cstdlib>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <iostream>
#include <pthread.h>

#define CAS(addr, oldval, newval) __sync_bool_compare_and_swap(addr, oldval, newval)
#define CAS64(addr, oldval, newval) __sync_bool_compare_and_swap((long *)addr, *(long *)&oldval, *(long *)&newval
#define CAS128(addr, oldval, newval) __sync_bool_compare_and_swap((__int128_t *)(addr), *(__int128_t *)&(oldval), *(__int128_t *)&(newval))
#define _DEBUG

using namespace std;

Page pages[PAGE_N];

MasterRecord ARIES_SYSTEM::master_record;

extern TransTable trans_table;
extern PageBufferEntry page_table[PAGE_N];

static std::mutex mr_mtx;
static int system_fd;

extern bool is_page_fixed(int page_id);
extern void page_fix(int page_id);
extern void append_transaction(Transaction trans);
extern void remove_transaction_xid(uint32_t xid);
extern void rollback(uint32_t xid);


static void load_master_record();
static void pbuf_lock_init();
static void recovery();
static void analysis();
static void redo();
static void undo();
static void page_undo_write(unsigned n);
static bool redo_test();
static void page_table_debug();


void 
ARIES_SYSTEM::db_init(){
  load_master_record();
  pbuf_lock_init();
}
static void 
pbuf_lock_init(){
  for(int i=0;i<PAGE_N;i++)
    pthread_rwlock_init(&page_table[i].lock, NULL);
}

static void
load_master_record(){
  trans_table.clear();

  if( (system_fd = open("/home/kamiya/hpcs/aries/data/system.dat", O_RDWR | O_CREAT )) == -1 ){
    perror("open");
    exit(1);
  }
  
  if( read(system_fd, &ARIES_SYSTEM::master_record, sizeof(MasterRecord)) == -1 ){
    perror("read");
    exit(1);
  }
  
  if(ARIES_SYSTEM::master_record.last_exit == false ) {
    cout << "[recovery start]" << endl;
    recovery();
  }

  ARIES_SYSTEM::master_record.last_exit = false;
  lseek(system_fd, 0, SEEK_SET);
  if( write(system_fd, &ARIES_SYSTEM::master_record, sizeof(MasterRecord)) == -1){
    perror("write");
    exit(1);
  }

  srand(time(NULL));
}

static void recovery(){
  analysis();
  redo();
  undo();
}

// Transactionテーブルをログから復元する
static void 
analysis(){
  int log_fd = open(Logger::logpath, O_CREAT | O_RDONLY);
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

  cout << "++++++++++++++++transaction table++++++++++++++++++" << endl;
  map<uint32_t, Transaction>::iterator it;
  for(it=trans_table.begin(); it!=trans_table.end(); it++){
    Transaction trans = it->second;
    cout << "+ [" << trans.TransID << "] " << "State=" << trans.State << ", LastLSN=" << trans.LastLSN << ", UndoNxtLSN=" << trans.UndoNxtLSN << endl;
  }
  cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++" << endl;

  close(log_fd);
}

static void 
redo(){
  memset(pages, 0, sizeof(pages));
  for(int i=0;i<PAGE_N;i++)
    pages[i].pageID = i+1;
  
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
      
      if(!is_page_fixed(idx))
	page_fix(idx);
      
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
    rollback(it->second.TransID);    
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
  if( (fd = open("/home/kamiya/hpcs/aries/data/pages.dat",  O_WRONLY )) == -1){
    perror("open");
    exit(1);
  }
  
  lseek(fd,0,SEEK_SET);

  for(unsigned i=0;i<n;i++){
    if( -1 == write(fd, &pages[i], sizeof(Page))){
      perror("write"); exit(1);
    }    
  }  
}

static bool 
redo_test(){
  int fd;
  
  if( (fd = open("/home/kamiya/hpcs/aries/data/pages.dat",  O_RDONLY )) == -1){
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

    if( p.pageID != pages[n].pageID || p.page_LSN != pages[n].page_LSN || p.value != pages[n].value ){
      return false;
    }
    n++;
  }

  return true;
}


uint32_t ARIES_SYSTEM::xid_inc(){

  int old, new_val;
  do {
    old = master_record.system_xid;
    new_val = old+1;
  }while(!CAS(&master_record.system_xid, old, new_val));

  /* normal_exit()時にまとめてマスターレコードを書き込む */

  // lseek(system_fd, 0, SEEK_SET);
  // if(write(system_fd, &master_record, sizeof(MasterRecord)) == -1){
  //   perror("write");
  //   exit(1);
  // };

  return new_val;

}

int ARIES_SYSTEM::abnormal_exit(){
  // master recordを書かない & トランザクションスレッドの終了を待機しない

  return 1;
}


int
ARIES_SYSTEM::normal_exit()
{

  // この関数に入る前に他スレッドが終了していること
  
  std::lock_guard<std::mutex> lock(mr_mtx);
  Logger::log_all_flush();

  master_record.last_exit=true;

  lseek(system_fd, 0, SEEK_SET);
  if(write(system_fd, &master_record, sizeof(MasterRecord)) == -1){
    perror("write");
    exit(1);
  };

  pthread_exit(NULL); // 他のスレッドが終了するのを待ってから、自スレッドを終了する. (unfixedスレッドモード用)

  return 1;
}

void ARIES_SYSTEM::transtable_debug(){
  cout << "++++++++++++++++transaction table++++++++++++++++++" << endl;
  map<uint32_t, Transaction>::iterator it;
  for(it=trans_table.begin(); it!=trans_table.end(); it++){
    Transaction trans = it->second;
    cout << "+ [" << trans.TransID << "] " << "State=" << trans.State << ", LastLSN=" << trans.LastLSN << ", UndoNxtLSN=" << trans.UndoNxtLSN << endl;
  }
  cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++" << endl;
}



