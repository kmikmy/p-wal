#include "ARIES.h"
#include <iostream>
#include <cstdlib>
#include <pthread.h>
#include <sched.h>
#include <map>
#include <set>

#define EX1
//#define EX10
// #define EX46

#define DEBUG 1

using namespace std;
enum T_Mode { COMMIT_M, UPDATE_M, ROLLBACK_M, FLUSH_M, SHOWDP_M };

// normal processing 用
DistributedTransTable *dist_trans_table;

extern map<uint32_t, uint32_t> dirty_page_table;

extern void operation_select(OP *op);
extern void page_select(uint32_t *page_id);
extern int update_operations(uint32_t xid, OP *ops, uint32_t *page_ids, int update_num, int th_id);
extern void begin(uint32_t,int);
extern void end(uint32_t,int);
extern void WAL_update(OP op, uint32_t xid, int page_id, int th_id);


std::istream& 
operator>>( std::istream& is, T_Mode& i )
{
  int tmp ;
  if ( is >> tmp )
    i = static_cast<T_Mode>( tmp ) ;
  return is ;
}

static void 
show_dp(){
  cout << endl << " *** show Dirty Page ***" << endl;
  map<uint32_t, uint32_t>::iterator it;
  for(it=dirty_page_table.begin(); it!=dirty_page_table.end(); it++){
    cout << " * " << it->first << ": " << it->second << endl;
  }
  cout << "***********************" << endl;
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
append_transaction(Transaction trans, int th_id){
  dist_trans_table[th_id] = trans;
}

void
clear_transaction(int th_id){
  memset(&dist_trans_table[th_id],0,sizeof(DistributedTransTable));
}

/* 
   Tranasction idとスレッド id を渡して、Transactionの実行を開始する。
   
   この関数ではまずトランザクションの内容を構築し、
   process_transaction()を呼び出す事で、トランザクションを処理する。
*/
void
start_transaction(uint32_t xid, int th_id) 
{
  /* 命令群を構成する */
  OP ops[MAX_UPDATE];
  uint32_t page_ids[MAX_UPDATE];
  int update_num = rand() % MAX_UPDATE + 1; // UPDATE 回数

#ifdef EX1
  update_num = 1;
#elif EX10
  update_num = 10;
#elif EX46
  update_num = 46;
#endif

  for(int i=0;i<update_num;i++){
    operation_select(&ops[i]);
    page_select(&page_ids[i]);
  }
  
  //  cout << "th_transaction: " << xid << endl;
  // transactionがrollbackしたら何度でも繰り返す
  while(update_operations(xid, ops, page_ids, update_num, th_id) == -1){
    clear_transaction(th_id); // 現在のトランザクションを一度終了させて
    Transaction trans;
    construct_transaction(&trans);
    xid = trans.TransID; // 新しいトランザクションIDで再度開始する.
  };

}

/*
  逐次でトランザクションを num 件実行する。
*/
void 
batch_start_transaction(int num){
  for(int i=0;i<num;i++){
    Transaction trans;
    construct_transaction(&trans);

    // th_id=0のdist_trans_tableを利用する
    append_transaction(trans, 0);
    start_transaction(trans.TransID, 0);
    clear_transaction(0);
  }
}

void
each_operation_mode(){
  Transaction trans;
  OP op;
  uint32_t page_id;
  int  m;

  construct_transaction(&trans);
  append_transaction(trans, 0);

  begin(trans.TransID,0);
  do {
    cout << "[UPDATE]:1, [END]:2, [EXIT]:0 ?" ;
    cin >> m;
    
    switch (m){
    case 1:
      operation_select(&op);
      page_select(&page_id);
      WAL_update(op, trans.TransID, page_id, 0);
      break;
    case 2: 
      end(trans.TransID,0);
      break;
    case 0: 
      break;
    default: cout << "Input Error! " << m << endl; 
    }
  } while(m && m!=2);

  clear_transaction(0);
}
