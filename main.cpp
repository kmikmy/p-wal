#include "ARIES.h"
#include <cstdlib>
#include <iostream>
#include <pthread.h>


enum Mode { NORMAL_EXIT=0, T_START=1, FAILURE=2 };
using namespace std;

extern void start_transaction(int);
extern pthread_t gen_queue_thread(int _ntrans);
extern void gen_pqueue_thread(int _nthread);

extern MasterRecord master_record;
extern TransTable trans_table;
extern TransQueue trans_queue;
extern PageBufferEntry pageBuffers[PAGE_N];

static void unfixed_thread_mode(int n);
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
    // 生成したスレッドの待ち合わせが難しい。ので廃止
    //unfixed_thread_mode(atoi(argv[1]));
  }
  else if(argc == 3){
    fixed_thread_mode(atoi(argv[1]), atoi(argv[2]));
  }

  return 0;
}


/*
  必要がないので廃止中。
*/
static void 
unfixed_thread_mode(int n){
  ARIES_SYSTEM::db_init();
  start_transaction(n);
  ARIES_SYSTEM::normal_exit();
}

static void 
fixed_thread_mode(int n,int nthread){
  ARIES_SYSTEM::db_init();

  /*
    gen_queue_thread()とgen_pqueue_thread()の呼び出し順は固定。
    gen_pqueue_thread()の中でpthread_join()が呼び出されているため。
 */
  pthread_t th = gen_queue_thread(n);
  gen_pqueue_thread(nthread);
  
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
    case T_START: start_transaction(1); break;
    case NORMAL_EXIT: ARIES_SYSTEM::normal_exit(); exit(0);
    case FAILURE: ARIES_SYSTEM::abnormal_exit(); exit(0);
    default: cout << "The mode isn't exist: " << m << endl; 
    }
  } while(m);
}

