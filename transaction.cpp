#include "include/ARIES.h"
#include "include/dpt.h"
#include "plugin/tpc-c/include/workload.h"
#include <iostream>
#include <cstdlib>
#include <pthread.h>
#include <sched.h>
#include <map>
#include <set>
#include <algorithm>
//#include "plugin/tpc-c/include/tpcc.h"

Constant g_c;

#define EX1
//#define EX10
// #define EX46

#define DEBUG 1

using namespace std;
enum T_Mode { COMMIT_M, UPDATE_M, ROLLBACK_M, FLUSH_M, SHOWDP_M };
extern int W;

// normal processing 用
DistributedTransTable *dist_trans_table;

extern void operationSelect(OP *op);
extern void pageSelect(uint32_t *page_id);
extern int updateOperations(uint32_t xid, OP *ops, uint32_t *page_ids, int update_num, int th_id);
extern void begin(uint32_t,int);
extern void end(uint32_t,int);
extern void WALUpdate(OP op, uint32_t xid, int page_id, int th_id);

std::istream&
operator>>( std::istream& is, T_Mode& i )
{
  int tmp ;
  if ( is >> tmp )
    i = static_cast<T_Mode>( tmp ) ;
  return is ;
}

uint32_t
constructTransaction(Transaction *trans)
{
    trans->TransID = ARIES_SYSTEM::xidInc();
    trans->State = U;
    trans->LastLSN = 0;
    trans->UndoNxtLSN = 0;

    return trans->TransID;
}

void
appendTransaction(Transaction trans, int th_id)
{
  dist_trans_table[th_id] = trans;
}

void
clearTransaction(int th_id)
{
  memset(&dist_trans_table[th_id],0,sizeof(DistributedTransTable));
}

/*
   Tranasction idとスレッド id を渡して、Transactionの実行を開始する。
   この関数ではまずトランザクションの内容を構築し、
   process_transaction()を呼び出す事で、トランザクションを処理する。
*/
void
startTransactionSimple(uint32_t xid, int th_id)
{
  /* 命令群を構成する */
  OP ops[MAX_UPDATE];
  uint32_t page_ids[MAX_UPDATE];
  int update_num=1;

  //  update_num = random() % MAX_UPDATE + 1; // UPDATE 回数

#ifdef EX1
  update_num = 1;
#endif
#ifdef EX10
  update_num = 10;
#endif
#ifdef EX46
  update_num = 46;
#endif

  for(int i=0;i<update_num;i++){
    if(i&1){
      ops[i].op_type = READ;
    } else {
      operationSelect(&ops[i]);
    }
    //    ops[i].op_type = READ;
    //    operationSelect(&ops[i]);
    pageSelect(&page_ids[i]);
  }

  sort(page_ids, page_ids+update_num); // デッドロックを起こさないようにするため、昇順にソートする

  //  cout << "th_transaction: " << xid << endl;
  // transactionがrollbackしたら何度でも繰り返す
  while(updateOperations(xid, ops, page_ids, update_num, th_id) == -1){
    clearTransaction(th_id); // 現在のトランザクションを一度終了させて
    Transaction trans;
    constructTransaction(&trans);
    xid = trans.TransID; // 新しいトランザクションIDで再度開始する.
  };
}

void
startTransactionNewOrder(uint32_t xid, int thId)
{
  XNewOrder x(xid, thId, g_c);
  x.run();
}

void
startTransaction(uint32_t xid, int thId)
{
  if(W > 0){
    startTransactionNewOrder(xid, thId);
  } else {
    startTransactionSimple(xid, thId);
  }
}
/*
  逐次でトランザクションを num 件実行する。
*/
void
batchStartTransaction(int num)
{
  for(int i=0;i<num;i++){
    Transaction trans;
    memset(&trans, 0, sizeof(Transaction));
    constructTransaction(&trans);

    // th_id=0のdist_trans_tableを利用する
    appendTransaction(trans, 0);
    //    start_transaction(trans.TransID, 0);
    startTransaction(trans.TransID, 0);
    clearTransaction(0);
  }
}

void
eachOperationMode(int file_id){
  Transaction trans;
  OP op;
  uint32_t page_id;
  int  m;

  memset(&trans, 0, sizeof(Transaction));
  constructTransaction(&trans);
  appendTransaction(trans, file_id);

  begin(trans.TransID,file_id);
  do {
    cout << "[UPDATE]:1, [END]:2, [EXIT]:0 ?" ;
    cin >> m;

    switch (m){
    case 1:
      operationSelect(&op);
      pageSelect(&page_id);
      WALUpdate(op, trans.TransID, page_id, file_id);
      break;
    case 2:
      end(trans.TransID,file_id);
      break;
    case 0:
      break;
    default: cout << "Input Error! " << m << endl;
    }
  } while(m && m!=2);

  clearTransaction(file_id);

  Logger::logAllFlush();
}
