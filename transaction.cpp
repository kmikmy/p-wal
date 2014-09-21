#include "ARIES.h"
#include <iostream>
#include <cstdlib>
#include <pthread.h>
#include <sched.h>
#include <map>

#define MAX_QUEUE_SIZE 32

/* processing thread の 最大数 */
#define MAX_PQUEUE_THREAD 15
//#define DEBUG 1

using namespace std;
enum T_Mode { COMMIT_M, UPDATE_M, ROLLBACK_M, FLUSH_M, SHOWDP_M };

/*
 When only fixed_thread_mode, trans_queue is used and 
 then these transactions in trans_queue will be moved
 to trans_table by processing transaction thread.
*/

TransTable trans_table;
TransQueue trans_queue; 

/* 
   thread_flagがtrueならまだタスクが生成される可能性がある。
   process_queue_threadはthread_flagとTransQueueの中身を見てexitするか判定する。
   manage_quqeue_threadはトランザクションの生成が全て終わると、このフラグを外す。
*/
static bool thread_flag; 


extern PageBufferEntry pageBuffers[PAGE_N];
extern map<uint32_t, uint32_t> DPT;

extern void* th_transaction(void *_xid);
extern void start_transaction(uint32_t xid, int th_id);
static void flush_page();

std::istream& 
operator>>( std::istream& is, T_Mode& i )
{
  int tmp ;
  if ( is >> tmp )
    i = static_cast<T_Mode>( tmp ) ;
  return is ;
}

static void 
create_tr_thread(uint32_t xid){
    pthread_t th;
    pthread_attr_t th_attr;
    pthread_attr_init(&th_attr);
    pthread_attr_setdetachstate(&th_attr , PTHREAD_CREATE_DETACHED);

    uint32_t *th_data = (uint32_t *)malloc(sizeof(uint32_t)); 
    // 動的に生成したデータでないと、create_tr_thread()のscopeを抜けたときに
    // xidのメモリも開放されてしまってトランザクションスレッドで正しくxidを
    // 読み込めないため。トランザクションスレッド側でfreeする。
    *th_data = xid;

    if( pthread_create(&th, &th_attr, th_transaction, (void *)th_data) != 0 ){
      perror("pthread_create()");
    }

    pthread_attr_destroy(&th_attr);
}

static void 
show_dp(){
  cout << endl << " *** show Dirty Page ***" << endl;
  map<uint32_t, uint32_t>::iterator it;
  for(it=DPT.begin(); it!=DPT.end(); it++){
    cout << " * " << it->first << ": " << it->second << endl;
  }
  cout << "***********************" << endl;
}

static std::mutex trans_table_mutex;

void 
append_transaction(Transaction trans){
  std::lock_guard<std::mutex> lock(trans_table_mutex);
  trans_table[trans.TransID] = trans;
}

void 
remove_transaction_xid(uint32_t xid){
  std::lock_guard<std::mutex> lock(trans_table_mutex);
  trans_table.erase(trans_table.find(xid));
}

void 
batch_start_transaction(int num){
  for(int i=0;i<num;i++){
    Transaction trans;
    trans.TransID = ARIES_SYSTEM::xid_inc();
    trans.State = U;
    trans.LastLSN = 0;
    trans.UndoNxtLSN = 0;
    append_transaction(trans);
    create_tr_thread(trans.TransID);
  }
}

uint32_t 
construct_transaction(Transaction *trans){
    trans->TransID = ARIES_SYSTEM::xid_inc();
    trans->State = U;
    trans->LastLSN = 0;
    trans->UndoNxtLSN = 0;

    return trans->TransID;
}

void 
flush_page(){
  int fd;
  if( (fd = open("data/pages.dat", O_CREAT | O_WRONLY )) == -1){
    perror("open");
    exit(1);
  }
  
  for(int i=0;i<PAGE_N;i++){
    if(!pageBuffers[i].fixed_flag)
      continue;

    lseek(fd,sizeof(Page)*pageBuffers[i].page_id, SEEK_SET);
    if( -1 == write(fd, &pageBuffers[i].page, sizeof(Page))){
      perror("write"); exit(1);
    }
  }    

  close(fd);
}

static
void *
manage_queue_thread(void *_ntrans){
  int ntrans = *((int *)_ntrans);
  int cnt = ntrans;
  free(_ntrans);

  int cpu = 0;
  cpu_set_t mask;
  /* initialize and set cpu flag */
  CPU_ZERO(&mask);
  CPU_SET(cpu, &mask);

  while(cnt > 0){
    trans_queue.lock(); // critical section start

    if(trans_queue.full()){
      //      cout << "full" << endl;
      trans_queue.unlock();
      //      usleep(1000);
      continue;
    }

    while(!trans_queue.full() && cnt > 0){
      Transaction trans;
      // transaction の構築
      construct_transaction(&trans);

      trans_queue.push(trans);
      cnt--;
#ifdef DEBUG
      cout << pthread_self() << ") cnt: " << cnt << endl;
#endif
    }

    trans_queue.unlock(); // critical section end
  }

  thread_flag = false; // process_queue_threadに終了を通知する 
  return NULL;
}

static
void *
process_queue_thread(void *_th_id){
  int th_id = *((int *)_th_id);
  free(_th_id);

  Transaction trans;

  int cpu = th_id+1;
  cpu_set_t mask;
  /* initialize and set cpu flag */
  CPU_ZERO(&mask);
  CPU_SET(cpu, &mask);



  /* queueにタスクがある or これからまだタスクが追加される　間はループ */
  while(!trans_queue.empty() || thread_flag){

    /* この間にqueueが空になる可能性がある */

    trans_queue.lock();    /* critical section start*/
    if(trans_queue.empty()){ // emptyなら少し待って再度上のwhileへ
      trans_queue.unlock(); /* critical section end */ 
      //      usleep(10);
      continue;
    }
    trans = trans_queue.front();
    trans_queue.pop();
    trans_queue.unlock(); /* critical section end */ 
    
    append_transaction(trans);

    // transactionの開始
    start_transaction(trans.TransID, th_id);
  }
  return NULL;
}



pthread_t
gen_queue_thread(int _ntrans){
    pthread_t th;

    trans_queue.setsize(MAX_QUEUE_SIZE);
    trans_queue.init();
    thread_flag = true;

    int *ntrans = (int *)malloc(sizeof(int)); 
    *ntrans = _ntrans;
    
    // 動的に生成したデータでないと、create_tr_thread()のscopeを抜けたときに
    // xidのメモリも開放されてしまってトランザクションスレッドで正しくxidを
    // 読み込めないため。トランザクションスレッド側でfreeする。

    if( pthread_create(&th, NULL, manage_queue_thread, (void *)ntrans) != 0 ){
      perror("pthread_create()");
      exit(1);
    }

    return th;
}


void
gen_pqueue_thread(int nthread){
    pthread_t th[MAX_PQUEUE_THREAD];
    
    for(int i=0; i<nthread; i++){
      int *_th_id = (int *)malloc(sizeof(int));
      *_th_id=i;

      if( pthread_create(&th[i], NULL, process_queue_thread, _th_id) != 0 ){
	perror("pthread_create()");
	exit(1);
      }
    }
    
    for(int i=0;i<nthread; i++){
      pthread_join(th[i], NULL);
      //cout << "thread(" << i << ")" << endl;
    }
    
}
