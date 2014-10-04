#include "ARIES.h"
#include <iostream>

//#define DEBUG

using namespace std;

/* QUEUEの長さ */
#define MAX_QUEUE_SIZE 1000000
/* processing thread の 最大数 */
#define MAX_WORKER_THREAD 15


/* 
   TransQueue:
   
   vectorでqueueを実装.
   std::queue.front()は参照を返すので、popで削除すると問題があるため.
*/
typedef class {
 private:
  unsigned max_size;
  pthread_mutex_t mutex;
  std::vector<Transaction> real_vector;

 public:
  int setsize(unsigned n)
  {
    if(n > MAX_QUEUE_SIZE){
      cout << "QUEUE_SIZE is too big" << endl;
      exit(1);
    }

    max_size = n;
    return 0;
  }

  void init()
  {
    pthread_mutex_init(&mutex,NULL);
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
    return real_vector.empty();
  }
  size_t size(){
    return real_vector.size();
  }
  bool full(){
    return size() >= max_size;
  }
  Transaction front(){
    return real_vector.front();
  }
  void pop(){
    real_vector.erase(real_vector.begin());
  }
  void push(const Transaction &trans){
    if(size() >= max_size)
      return;

    real_vector.push_back(trans);
  }

} TransQueue;


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
gen_producer_thread(int _ntrans){
    pthread_t th;

    trans_queue.setsize(MAX_QUEUE_SIZE);
    trans_queue.init();
    rest_flag = true;

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
      //cout << "thread(" << i << ")" << endl;
    }
    
}
