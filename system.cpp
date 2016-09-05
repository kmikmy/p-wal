#include "include/ARIES.h"
#include "include/schema.h"
#include <mutex>
#include <cstdlib>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <iostream>
#include <exception>
#include <pthread.h>
#include <fstream>

#define _DEBUG

//#define ANALYSIS

using namespace std;

MasterRecord ARIES_SYSTEM::master_record;
extern uint64_t cas_miss[MAX_WORKER_THREAD];


extern TransTable recovery_trans_table;
extern DistributedTransTable *dist_trans_table;

extern BufferControlBlock page_table[PAGE_N];
extern char* ARIES_HOME;

static std::mutex mr_mtx;
static int system_fd;

extern void recovery();
extern void pbufInit();

extern void tpcc_init();
extern void tpcc_load();

extern void mt19937Init();

static void
createDistTransTable(int th_num)
{
  dist_trans_table = (DistributedTransTable *)calloc(th_num, sizeof(DistributedTransTable));
}

static void
loadMasterRecord(){
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
//    recovery_trans_table.clear();
//    recovery();
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
ARIES_SYSTEM::dbInit(int th_num)
{
  // マスターレコードの作成
  loadMasterRecord();
  // 全ページをメモリバッファに読み込んで、ロックオブジェクトを初期化する
  pbufInit();
  // 分散トランザクションテーブルの生成
  createDistTransTable(th_num);
  // loggerの初期化
  Logger::init();
  // tpc-c
  tpcc_init();
  tpcc_load();
  // mt19937
  mt19937Init();

  loadAllSchema();
}

uint32_t
ARIES_SYSTEM::xidInc()
{
  return __sync_fetch_and_add(&master_record.system_xid,1);
}

uint32_t
ARIES_SYSTEM::xidRead()
{
  return master_record.system_xid;
}

int
ARIES_SYSTEM::abnormalExit()
{
  // master recordを書かない & トランザクションスレッドの終了を待機しない
  return 1;
}


int
ARIES_SYSTEM::normalExit()
{
  // この関数に入る前に他スレッドが終了していること
  std::lock_guard<std::mutex> lock(mr_mtx);
  Logger::logAllFlush();

  master_record.last_exit=true;

  lseek(system_fd, 0, SEEK_SET);

  if(write(system_fd, &master_record, sizeof(MasterRecord)) == -1){
    perror("write");
    exit(1);
  };

#ifdef ANALYSIS
  uint64_t total_cas_miss=0;
  for(int i=0;i<MAX_WORKER_THREAD;i++){
    total_cas_miss += cas_miss[i];
  }
  std::ofstream ofs("cas_missed.csv", std::ios::out | std::ios::app);
  ofs << total_cas_miss << ",";

  pid_t pid;
  char buf[128];
  pid = getpid();

  snprintf(buf, sizeof(buf), "cat /proc/%d/status | grep '^voluntary_ctxt_switches' | awk '{print $2}' | tr '\n' ',' >> voluntary_ctxt_switches.csv", pid);
  system(buf);

  snprintf(buf, sizeof(buf), "cat /proc/%d/status | grep nonvoluntary_ctxt_switches | awk '{print $2}' | tr '\n' ',' >> nonvoluntary_ctxt_switches.csv", pid);
  system(buf);
#endif

  return 1;
}

void
ARIES_SYSTEM::transtableDebug()
{
  cout << "++++++++++++++++transaction table++++++++++++++++++" << endl;
  TransTable::iterator it;
  // for(it=recovery_trans_table.begin(); it!=recovery_trans_table.end(); it++){
  //   Transaction trans = it->second;
  //   cout << "+ [" << trans.TransID << "] " << "State=" << trans.State << ", LastLSN=" << trans.LastLSN << ", UndoNxtLSN=" << trans.UndoNxtLSN << endl;
  // }
  cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++" << endl;
}
