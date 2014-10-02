#include "ARIES.h"
#include <cstdlib>
#include <iostream>
#include <pthread.h>

#define NUM_MAX_CORE 16

enum Mode { NORMAL_EXIT=0, T_START=1, FAILURE=2 };
using namespace std;

extern void batch_start_transaction(int);
extern pthread_t gen_producer_thread(int _ntrans);
extern void gen_worker_thread(int _nthread);

extern MasterRecord master_record;
extern TransTable trans_table;
extern TransQueue trans_queue;
extern PageBufferEntry pageBuffers[PAGE_N];

static void fixed_thread_mode(int n, int nthread);
static void interact_mode();

std::istream& operator>>( std::istream& is, Mode& i )
{
  int tmp;
  if ( is >> tmp )
    i = static_cast<Mode>( tmp ) ;
  return is ;
}

int main(int argc, char *argv[]){

  if(argc == 1){
    interact_mode();
  }
  else if(argc == 2){
    cout << "usage: ./aries num_trans num_threads" << endl;
  }
  else if(argc == 3){
    int th_num = atoi(argv[2]);
    int cpunum = sysconf(_SC_NPROCESSORS_ONLN);

    if(th_num > NUM_MAX_CORE-1 || th_num > cpunum-1){
      cout << "Usage: ./a.out xact_num th_num(<=" << (NUM_MAX_CORE<cpunum?NUM_MAX_CORE-1:cpunum-1) << ")" << endl;
      return 0;
    }
    fixed_thread_mode(atoi(argv[1]), atoi(argv[2]));
  }

  return 0;
}

static void 
fixed_thread_mode(int n,int nthread){
  ARIES_SYSTEM::db_init();
  /*
    gen_producer_thread()とgen_worker_thread()の呼び出し順は固定。
    gen_worker_thread()の中でworkerスレッドに対するpthread_join()が呼び出されているため。
  */
  pthread_t th = gen_producer_thread(n);
  gen_worker_thread(nthread);
  
  pthread_join(th, NULL);

  ARIES_SYSTEM::normal_exit();
}

static
void interact_mode(){

  ARIES_SYSTEM::db_init();

#ifdef DEBUG
  cout << "DB CONNECTTED" << endl;
  cout << "The number of transaction is " << trans_table.tt_header.tnum << endl;

#endif 
  cout << "system_xid is " << ARIES_SYSTEM::master_record.system_xid << endl;

  Mode  m;
  do {
    cout << "[NORMAL EXIT]:0, [Trasaction Start]:1, [FAILURE]:2? " ;
    cin >> m;
    
    switch (m){
    case T_START: batch_start_transaction(1); break;
    case NORMAL_EXIT: ARIES_SYSTEM::normal_exit(); exit(0);
    case FAILURE: ARIES_SYSTEM::abnormal_exit(); exit(0);
    default: cout << "The mode isn't exist: " << m << endl; 
    }
  } while(m);
}

