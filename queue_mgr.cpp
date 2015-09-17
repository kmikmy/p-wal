#include "include/ARIES.h"
#include <sys/time.h>
#include <iostream>

//#define DEBUG

using namespace std;

/* 
   QUEUEの長さ 
   一つのキュー当たり最大で MAX_QUEUE_SIZE * 512バイト のログが書かれる.(LogHeaderのサイズは含まない)
   QUEUEがemptyの状態とfullの状態を区別するために、fullの状態は１つ分だけ要素が空いているようにするので、
   キューに入る最大要素+1が実際のキューのサイズとする.
*/
#define MAX_QUEUE_SIZE 10000000 + 1 
//#define MAX_QUEUE_SIZE 100 + 1 

extern MasterRecord master_record;

/* 
   TransQueue:
*/
class TransQueue{
private:
  pthread_mutex_t mutex;
  // std::vector<Transaction> real_vector;
  Transaction real_queue[MAX_QUEUE_SIZE]; // fullとemptyを区別するために1要素余分に保持する
  int head, next; //先頭と最後の番号を指す
  
public:
  TransQueue(){
    init();
  }
  void init()
  {
    head = next = 0;
  }
  void lock()
  {
    pthread_mutex_lock(&mutex);
  }
  void unlock()
  {
    pthread_mutex_unlock(&mutex);
  }

  bool empty(){
    return head == next;
  }
  size_t size(){
    return (head<=next) ? next-head :(MAX_QUEUE_SIZE-head)+next;
  }

  /* emptyの状態と区別するために、一要素分開けた状態をfullの定義とする */
  bool full(){
    return size() == MAX_QUEUE_SIZE-1;
  }
  Transaction front(){
    return real_queue[head];
  }
  void pop(){
    head++;
    if(head==MAX_QUEUE_SIZE)
      head = 0;
  }
  void push(Transaction trans){
    if(full()){
      cout << "trans_queue is full." << endl;
      exit(1);
    }

    real_queue[next++]=trans;
    if(next==MAX_QUEUE_SIZE)
      next = 0;
  }

};

typedef struct _ProArg{
  int ntrans; 
  int nqueue;
} ProArg;


extern uint32_t construct_transaction(Transaction *trans);
extern void start_transaction(uint32_t xid, int th_id);
extern void  append_transaction(Transaction trans, int th_id);
extern void clear_transaction(int th_id);

#ifdef BATCH_TEST
/************************************************************************
 * トランザクションキューに一度にまとめてトランザクションをセットする時のみ使う関数
 ***********************************************************************/
static void trans_queues_init(uint32_t ntrans, uint32_t nqueue);

#else
/************************************************************************
 * トランザクションキューに逐次トランザクションを追加する時のみ使う関数の宣言
 ***********************************************************************/
static void *manage_queue_thread(void *_args);
#endif

TransQueue *dist_trans_queues;

/* 
   rest_flagがtrueならまだタスクが生成される可能性がある。
   process_queue_threadはrest_flagとTransQueueの中身を見てexitするか判定する。
   manage_quqeue_threadはトランザクションの生成が全て終わると、このフラグを外す。
*/
static bool rest_flag; 

static void
create_dist_trans_queues(int th_num){
  dist_trans_queues = (TransQueue *)calloc(th_num, sizeof(TransQueue));
}

static
void *
process_queue_thread(void *_th_id){
  int th_id = *((int *)_th_id);
  //  free(_th_id);

  Transaction trans;

  int cpu = th_id+1;
  cpu_set_t mask;

  /* initialize and set cpu flag */
  CPU_ZERO(&mask);
  CPU_SET(cpu, &mask);

  /* set affinity to current process */
  //  sched_setaffinity(0, sizeof(mask), &mask);

  int queue_id = th_id;
#ifndef BATCH_TEST
  queue_id = 0; // 全てのスレッドで同じキューを見る。その単一キューにcreaterスレッドがエントリをプッシュしていく。
#endif

  /* queueにタスクがある or これからまだタスクが追加される　間はループ */
  while(!dist_trans_queues[queue_id].empty() || rest_flag){
    /* この間にqueueが空になる可能性がある */
    dist_trans_queues[queue_id].lock();    /* critical section start*/

    if(dist_trans_queues[queue_id].empty()){ // emptyなら少し待って再度上のwhileへ
      dist_trans_queues[queue_id].unlock(); /* critical section end */ 
      //      usleep(10);
      continue;
    }

    trans = dist_trans_queues[queue_id].front();
    dist_trans_queues[queue_id].pop();

    dist_trans_queues[queue_id].unlock(); /* critical section end */ 
    
    append_transaction(trans, th_id);
    // transactionの開始
    start_transaction(trans.TransID, th_id);
    clear_transaction(th_id);

  }
  return NULL;
}

/* batchモードの時に渡す。 何もしない関数 */
void*
dummy_func(void *){
  return NULL;
}

pthread_t
gen_producer_thread(int _ntrans, int _nqueue){
    pthread_t th;
    rest_flag = true;

    create_dist_trans_queues(_nqueue);

#ifdef BATCH_TEST
    trans_queues_init(_ntrans, _nqueue);
    rest_flag = false;
    if( pthread_create(&th, NULL, dummy_func, NULL) != 0 ){
      perror("pthread_create()");
      exit(1);
    }
    return th;

#else
    ProArg *args = (ProArg *)malloc(sizeof(ProArg)); 
    args->ntrans = _ntrans;
    args->nqueue = _nqueue;
    
    // 動的に生成したデータでないと、create_tr_thread()のscopeを抜けたときに
    // xidのメモリも開放されてしまってトランザクションスレッドで正しくxidを
    // 読み込めないため。トランザクションスレッド側でfreeする。

    if( pthread_create(&th, NULL, manage_queue_thread, (void *)args) != 0 ){
      perror("pthread_create()");
      exit(1);
    }
    return th;
#endif

}


void
gen_worker_thread(int nthread){
    pthread_t th[MAX_WORKER_THREAD];

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
    }
}




#ifdef BATCH_TEST
/************************************************************************
 * トランザクションキューに一度にまとめてトランザクションをセットする時のみ使う関数
 ***********************************************************************/

/* 
   trans_queuesにトランザクションをまとめて詰め込む。
 */
static 
void 
trans_queues_init(uint32_t ntrans, uint32_t nqueue){
#ifdef DEBUG
  cout << "[creating trans_queue]" << endl;
#endif

  uint32_t n_each_queue = ntrans / nqueue;
  uint32_t reminder = ntrans % nqueue;

  Transaction trans;
  construct_transaction(&trans);

  for(uint32_t i=0;i<n_each_queue;i++){
    for(uint32_t j=0;j<nqueue;j++){
      dist_trans_queues[j].push(trans);
      trans.TransID++;
    }
  }

  for(uint32_t j=0;j<reminder;j++){
    dist_trans_queues[j].push(trans);
    trans.TransID++;
  }

  // 現在までに与えた最後のXIDをマスタレコードに保持する.
  ARIES_SYSTEM::master_record.system_xid = trans.TransID;
  //  struct timeval t;
  //  gettimeofday(&t,NULL);


#ifdef DEBUG  
  cout << "[start processing]" << endl;
#endif

  return;
}

#else

/************************************************************************
 * トランザクションキューに逐次トランザクションを追加する時のみ使う関数
 ***********************************************************************/

static
void *
manage_queue_thread(void *_args){
  ProArg args = *((ProArg *)_args);
  int cnt = args.ntrans;
  //  int nqueue = args.nqueue;
  //   free(_args);

  int cpu = 0;
  cpu_set_t mask;

  /* initialize and set cpu flag */
  CPU_ZERO(&mask);
  CPU_SET(cpu, &mask);

  /* set affinity to current process */
  //  sched_setaffinity(0, sizeof(mask), &mask);

  while(cnt > 0){
    dist_trans_queues[0].lock(); // critical section start

    // 一度ロックを取ったらtrans_queueが一杯になるまでトランザクションをキューにプッシュする
    while(!dist_trans_queues[0].full() && cnt > 0){ 
      Transaction trans;
      construct_transaction(&trans);
      dist_trans_queues[0].push(trans);

      cnt--;

#ifdef DEBUG
      cout << pthread_self() << ") cnt: " << cnt << endl;
#endif
    }

    if(dist_trans_queues[0].full()){ 
      //      cout << "trans_queue is full" << endl;
    }

    dist_trans_queues[0].unlock(); // critical section end
    //      usleep(1000);
  }
  rest_flag = false; // process_queue_threadに終了を通知する 

  return NULL;
}

#endif

