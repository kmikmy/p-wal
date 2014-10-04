#include "ARIES.h"
#include <iostream>

//#define DEBUG

using namespace std;

/* 
   QUEUEの長さ 
   一つのキュー当たり最大で 1千万 * 120バイト = 1.2GB のログが書かれる. 
   QUEUEがemptyの状態とfullの状態を区別するために、fullの状態は１つ分だけ要素が空いているようにするので、
   キューに入る最大要素+1が実際のキューのサイズとする.
*/
//#define MAX_QUEUE_SIZE 10000000 + 1 
#define MAX_QUEUE_SIZE 100 + 1 
/* processing thread の 最大数 */
#define MAX_WORKER_THREAD 7


/* 
   TransQueue:
   
   vectorでqueueを実装.
   std::queue.front()は参照を返すので、popで削除すると問題があるため.
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
extern void  append_transaction(Transaction trans);

TransQueue trans_queue;
TransQueue trans_queues[MAX_WORKER_THREAD]; 

/* 
   rest_flagがtrueならまだタスクが生成される可能性がある。
   process_queue_threadはrest_flagとTransQueueの中身を見てexitするか判定する。
   manage_quqeue_threadはトランザクションの生成が全て終わると、このフラグを外す。
*/
static bool rest_flag; 


/* 
   trans_queuesにトランザクションをまとめて詰め込む。
 */
static 
void *
trans_queues_init(uint32_t ntrans){
  return NULL;
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

  /* set affinity to current process */
  sched_setaffinity(0, sizeof(mask), &mask);

  while(cnt > 0){
    trans_queue.lock(); // critical section start

    // 一度ロックを取ったらtrans_queueが一杯になるまでトランザクションをキューにプッシュする
    while(!trans_queue.full() && cnt > 0){ 
      Transaction trans;
      construct_transaction(&trans);
      trans_queue.push(trans);

      cnt--;
#ifdef DEBUG
      cout << pthread_self() << ") cnt: " << cnt << endl;
#endif
    }

    if(trans_queue.full()){ 
      //      cout << "trans_queue is full" << endl;
    }

    trans_queue.unlock(); // critical section end
    //      usleep(1000);
  }
  rest_flag = false; // process_queue_threadに終了を通知する 

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

  /* set affinity to current process */
  sched_setaffinity(0, sizeof(mask), &mask);

  /* queueにタスクがある or これからまだタスクが追加される　間はループ */
  while(!trans_queue.empty() || rest_flag){
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
gen_producer_thread(int _ntrans, int _nqueue){
    pthread_t th;

    trans_queue.init();
    rest_flag = true;

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
}


void
gen_worker_thread(int nthread){
    pthread_t th[MAX_WORKER_THREAD];

    Logger::init();
    
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
