#include "ARIES.h"
#include <mutex>
#include <cstdlib>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <iostream>
#include <exception>
#include <pthread.h>

#define CAS(addr, oldval, newval) __sync_bool_compare_and_swap(addr, oldval, newval)
#define CAS64(addr, oldval, newval) __sync_bool_compare_and_swap((long *)addr, *(long *)&oldval, *(long *)&newval
#define CAS128(addr, oldval, newval) __sync_bool_compare_and_swap((__int128_t *)(addr), *(__int128_t *)&(oldval), *(__int128_t *)&(newval))
#define _DEBUG

using namespace std;

MasterRecord ARIES_SYSTEM::master_record;

extern TransTable recovery_trans_table;
extern DistributedTransTable *dist_trans_table;

extern BufferControlBlock page_table[PAGE_N];
extern map<uint32_t, uint32_t> dirty_page_table;
extern char* ARIES_HOME;

static std::mutex mr_mtx;
static int system_fd;

extern void recovery();
extern void pbuf_init();

static void
create_dist_trans_table(int th_num)
{
  dist_trans_table = (DistributedTransTable *)calloc(th_num, sizeof(DistributedTransTable));
}

static void
load_master_record(){
  std::string mr_filename = ARIES_HOME;
  mr_filename += "/data/system.dat";

  if( (system_fd = open(mr_filename.c_str(), O_RDWR | O_CREAT )) == -1 ){
    perror("open");
    exit(1);
  }
  
  if( read(system_fd, &ARIES_SYSTEM::master_record, sizeof(MasterRecord)) == -1 ){
    perror("read");
    exit(1);
  }
  
  if(ARIES_SYSTEM::master_record.last_exit == false ) {
    cout << "[recovery start]" << endl;
    recovery_trans_table.clear();
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

void 
ARIES_SYSTEM::db_init(int th_num){
  // マスターレコードの作成
  load_master_record();
  // 全ページをメモリバッファに読み込んで、ロックオブジェクトを初期化する
  pbuf_init();
  // 分散トランザクションテーブルの生成
  create_dist_trans_table(th_num);
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

uint32_t ARIES_SYSTEM::xid_read(){
  return master_record.system_xid;
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

  return 1;
}

void ARIES_SYSTEM::transtable_debug(){
  cout << "++++++++++++++++transaction table++++++++++++++++++" << endl;
  map<uint32_t, Transaction>::iterator it;
  for(it=recovery_trans_table.begin(); it!=recovery_trans_table.end(); it++){
    Transaction trans = it->second;
    cout << "+ [" << trans.TransID << "] " << "State=" << trans.State << ", LastLSN=" << trans.LastLSN << ", UndoNxtLSN=" << trans.UndoNxtLSN << endl;
  }
  cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++" << endl;
}



