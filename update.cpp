#include "include/ARIES.h"
#include "include/schema.h"
#include <iostream>
#include <cstdlib>
#include <ctime>
#include <map>
#include <set>
#include <sys/time.h>
#include <pthread.h>
//#include "plugin/tpc-c/include/tpcc.h"

// #define READ_MODE 1

using namespace std;

enum UP_OPTYPE { _EXIT, _INC, _DEC, _SUBST, _SYSTEM_FAILURE };

const uint32_t Delta = 99999; // Delta(ms)でロックが獲得できない場合はrollbackする。

extern DistributedTransTable *dist_trans_table;


extern BufferControlBlock page_table[PAGE_N];
extern char *ARIES_HOME;

extern void page_fix(int page_id, int th_id);

void WAL_update(OP op, uint32_t xid, int page_id, int th_id);
void begin_checkpoint();
void begin(uint32_t xid, int th_id);
void end(uint32_t xid, int th_id);
void rollback(uint32_t xid, int th_id);



std::istream& 
operator>>( std::istream& is, UP_OPTYPE& i )
{
  int tmp ;
  if ( is >> tmp )
    i = static_cast<UP_OPTYPE>( tmp ) ;
  return is ;
}

void
operation_select(OP *op){
    int tmp = rand() % 3 + 1;
    switch(tmp){
    case 1: op->op_type = INC; break;
    case 2: op->op_type = DEC; break; 
    case 3: op->op_type = SUBST; break;
    default : op->op_type = INC; break;
    }
    op->amount = rand() % 100 + 1;
}

void
page_select(uint32_t *page_id){
  *page_id = (uint32_t)PAGE_N*((double)rand()/((double)RAND_MAX+1.0));

}

static void
lock_release(set<uint32_t> &lock_table){
  for(set<uint32_t>::iterator it = lock_table.begin(); it!=lock_table.end(); it++){
    pthread_rwlock_unlock(&page_table[*it].lock);
  }
}


int
update_operations(uint32_t xid, OP *ops, uint32_t *page_ids, int update_num, int th_id){
  set<uint32_t> my_lock_table;
  OP op;
  uint32_t page_id;
  int read_value;

#ifndef READ_MODE
  begin(xid, th_id);  // begin log write  
#endif
  for(int i=0;i<update_num;i++){
    op = ops[i];
    page_id = page_ids[i];

    BufferControlBlock *pbuf = &(page_table[page_id]);

    /* 
       自スレッドが該当ページのロックを取得していないなら、グローバルロックテーブルにロックをかけて、
       他に該当ページを更新するスレッドが先にいないかをチェックする。
     */
    if(my_lock_table.find(page_id) == my_lock_table.end()){ 
      /* 自スレッドが該当ページのロックをまだ獲得していない状態 */
      
      // 開始時間の計測
      struct timeval s,t;
      gettimeofday(&s,NULL);
      while(1){

	if(op.op_type == READ){
	  if(pthread_rwlock_tryrdlock(&pbuf->lock) == 0) break; // 読み込みロックの獲得に成功したらbreakする。
	} else {
	  if(pthread_rwlock_trywrlock(&pbuf->lock) == 0) break; // 書き込みロックの獲得に成功したらbreakする。
	}
	/*
	  開始時からの経過時間の計測.
	  δ以上経過してたら、打ち切ってロールバックする.
	*/
	gettimeofday(&t,NULL);	
	/* May be, deadlock occur. */
	if( ((t.tv_sec - s.tv_sec)*1000 + (t.tv_usec - s.tv_usec)/1000 ) > Delta ){
	  cout << "-" ;
	  rollback(xid, th_id);

	  lock_release(my_lock_table);
	  return -1;
	} 
	else{
	  //	  cout << "wait:" << xid << endl;
	}
      }
      
      //lockが完了したら、ロックテーブルに追加
      my_lock_table.insert(page_id);
    }
    else{ 
      ; //既に該当ページのロックを獲得している場合は何もしない
    }
    
    if(op.op_type == READ){
      read_value = page_table[page_id].page.value;
      read_value++; // * this is just codes for avoiding warning
    } else {
      WAL_update(op, xid, page_id, th_id);
    }
  }
#ifndef READ_MODE
  end(xid, th_id); // end log write
#endif
  lock_release(my_lock_table);

  return 0;
}

void WAL_update(OP op, uint32_t xid, int page_id, int th_id){
  BufferControlBlock *pbuf = &page_table[page_id];
  // fixed flagがなければファイルから読み込んでfixする

#ifdef DEBUG
  debug("fixed_count: " << pbuf->fixed_count << endl);
#endif

  page_fix(page_id, th_id); // pageがBCBにfixされていなかったらfix。既にfixされている場合はfixed_countを+1する。

  Log log;
  //   LSNはログを書き込む直前に決定する
  log.trans_id = xid;
  log.type = UPDATE;
  log.page_id = page_id;

  log.prev_lsn = dist_trans_table[th_id].LastLSN;
  log.prev_offset = dist_trans_table[th_id].LastOffset;
  log.undo_nxt_lsn = 0; // UndoNxtLSNがログに書かれるのはCLRのみ.
  log.undo_nxt_offset = 0;
  //  log.before = pbuf->page.value;

  switch(op.op_type){
  case INC: pbuf->page.value += op.amount; break;
  case DEC: pbuf->page.value -= op.amount; break;
  case SUBST: pbuf->page.value = op.amount; break;
  default: break;
  }
  //  log.after = pbuf->page.value;

  //  ARIES_SYSTEM::transtable_debug();

  Logger::log_write(&log, NULL, th_id);
  pbuf->page.page_LSN = log.lsn;


  dist_trans_table[th_id].LastLSN = log.lsn;
  dist_trans_table[th_id].LastOffset = log.offset;
  dist_trans_table[th_id].UndoNxtLSN = log.lsn; // undoできる非CLRレコードの場合はUndoNxtLSNはLastLSNと同じになる
  dist_trans_table[th_id].UndoNxtOffset = log.offset;

  //  cout << "[Before Update]" << endl;
  //  cout << "page[" << p.page_id << "]: page_LSN=" << p.page_LSN << ", value=" << p.value << endl;

  /*  毎更新時にディスク上のページへforceしないのでコメントアウト */
  //  lseek(fd,sizeof(Page)*page_id,SEEK_SET);
  //  if( -1 == write(fd, &p, sizeof(Page))){
  //    perror("write"); exit(1);
  //  }    

  //  cout << "[After Update]" << endl;
  //  cout << "page[" << p.page_id << "]: page_LSN=" << p.page_LSN << ", value=" <<  p.value << endl;  
}

void 
begin(uint32_t xid, int th_id=0){
  Log log;
  memset(&log,0,sizeof(log));
  log.trans_id = xid;
  log.type = BEGIN;

  Logger::log_write(&log, NULL, th_id);

  dist_trans_table[th_id].TransID = xid;
  dist_trans_table[th_id].LastLSN = log.lsn;
  dist_trans_table[th_id].LastOffset = log.offset;
  dist_trans_table[th_id].UndoNxtLSN = log.lsn; /* BEGIN レコードは END レコードによってコンペンセーションされる. */
  dist_trans_table[th_id].LastOffset = log.offset;


#ifdef DEBUG
  Logger::log_debug(log);
#endif
}

void
end(uint32_t xid, int th_id=0){
  Log log;
  memset(&log,0,sizeof(log));
  log.trans_id = xid;
  log.type = END;
  log.prev_lsn = dist_trans_table[th_id].LastLSN;
  log.prev_offset = dist_trans_table[th_id].LastOffset;
  log.undo_nxt_lsn = 0; /* 一度 END ログが書かれたトランザクションは undo されることはない */
  log.undo_nxt_offset = 0;

  Logger::log_write(&log, NULL, th_id);

  /* dist_transaction_tableのエントリを削除する. */
  memset(&dist_trans_table[th_id], 0, sizeof(DistributedTransTable));

#ifdef DEBUG
  Logger::log_debug(log);
#endif
}

void
update(const char* table_name, QueryArg *q, int update_field_num, uint32_t page_id, uint32_t xid, uint32_t thId){
  Log log;
  FieldLogList *p;
  TSchema *tableSchema = MasterSchema::getTableSchemaPtr(table_name);

  //  lock()を取りたい．

  //   LSNはログを書き込む直前に決定する
  log.trans_id = xid;
  log.type = UPDATE;
  log.page_id = page_id;

  log.prev_lsn = dist_trans_table[thId].LastLSN;;
  log.prev_offset = dist_trans_table[thId].LastOffset;
  log.undo_nxt_lsn = 0; // UndoNextLSNがログに書かれるのはCLRのみ.
  log.undo_nxt_offset = 0;

  /* 以下からTPC-C用に適用したフィールド */
  strncpy(log.table_name, table_name, strlen(table_name)+1);
  log.field_num = update_field_num;

  size_t total_field_length=0;
  try{
    p = new FieldLogList[update_field_num];
  } catch(std::bad_alloc e){
    PERR("new");
  }
  for(int i=0;i < update_field_num;i++, q = q->nxt){
    if(i > 0){
      p[i-1].nxt = &p[i];
    }

    FInfo finfo = tableSchema->getFieldInfo(q->field_name);
    p[i].fieldOffset = finfo.offset;
    p[i].fieldLength = finfo.length;
    p[i].before = q->before;
    p[i].before = q->after;
    p[i].nxt = NULL;

    total_field_length += sizeof(size_t)*2 + finfo.length*2;
  }
  log.total_field_length = total_field_length;
  log.total_length = total_field_length + sizeof(LogRecordHeader);

  Logger::log_write(&log, p, thId);

  delete[] p;
}

/*
  rollback()内ではトランザクションテーブルからエントリを削除しない.
  Iteratorを使って、トランザクションテーブルを巡回している場合があるため.
  rollback中のトランザクションテーブルの更新が不適(2014/11/14現在).
  recovery.cppのrollback_for_recoveryのコードをコピーする予定.

  前に書かれたログが書き込まれていない可能性がある。
  そのために、一度ログバッファの中身をflushしなければならない（？　#2015/07/09
*/
void
rollback(uint32_t xid, int th_id){
  //  cout << "rollback[" << xid << "]" << std::endl;

  int log_fd = open(Logger::logpath, O_RDONLY);
  if(log_fd == -1){
    perror("open"); exit(1);
  }

  Logger::log_flush(th_id);
  
  uint64_t log_offset = dist_trans_table[th_id].LastOffset; // rollbackするトランザクションの最後のLSN

  Log log;
  while(log_offset != 0){ // lsnが0になるのはprevLSNが0のBEGINログを処理した後
    lseek(log_fd, log_offset, SEEK_SET);

    int ret = read(log_fd, &log, sizeof(Log));
    if(ret == -1){
      perror("read"); exit(1);
    }

    if(ret == 0){
      cout << "illegal read" << endl;
      exit(1);
    }

#ifdef DEBUG
    Logger::log_debug(log);
#endif

    if (log.type == UPDATE || log.type == INSERT){
      int idx=log.page_id;

      if(log.type == UPDATE){
	switch((kTableType)log.table_id){
	case SIMPLE:
	  //	  page_table[idx].page.value = log.before;
	  page_table[idx].page.page_LSN = log.prev_lsn;
	  break;
	case WAREHOUSE:
	  //	  memcpy(&Warehouse::pages[idx-1], log.padding, sizeof(PageWarehouse));
	  //  Warehouse::pages[idx-1].page_LSN = log.prev_lsn;
	  break;
	case DISTRICT:
	  //	  memcpy(&District::pages[idx-1], log.padding, sizeof(PageDistrict));
	  //	  District::pages[idx-1].page_LSN = log.prev_lsn;
	  break;
	case CUSTOMER:
	  //	  memcpy(&Customer::pages[idx-1], log.padding, sizeof(PageCustomer));
	  //	  Customer::pages[idx-1].page_LSN = log.prev_lsn;
	  break;
	case HISTORY:
	  /* not defined */
	  break;
	case ORDER:
	  // memcpy(&Order::pages[idx-1], log.padding, sizeof(PageOrder));
	  //	  Order::pages[idx-1].page_LSN = log.prev_lsn;
	  break;
	case ORDERLINE:
	  // memcpy(&OrderLine::pages[idx-1], log.padding, sizeof(PageOrderLine));
	  //	  OrderLine::pages[idx-1].page_LSN = log.prev_lsn;
	  break;
	case NEWORDER:
	  // memcpy(&NewOrder::pages[idx-1], log.padding, sizeof(PageNewOrder));
	  //	  NewOrder::pages[idx-1].page_LSN = log.prev_lsn;
	  break;
	case ITEM:
	  //	  memcpy(&Item::pages[idx-1], log.padding, sizeof(PageItem));
	  //	  Item::pages[idx-1].page_LSN = log.prev_lsn;
	  break;
	case STOCK:
	  //	  memcpy(&Stock::pages[idx-1], log.padding, sizeof(PageStock));
	  //	  Stock::pages[idx-1].page_LSN = log.prev_lsn;
	  break;
	default:
	  PERR("switch");
	}
      } if(log.type == INSERT){
	switch((int)log.table_id){
	case SIMPLE:
	  break;
	case WAREHOUSE:
	  break;
	case DISTRICT:
	  break;
	case CUSTOMER:
	  break;
	case HISTORY:
	  /* not defined */
	  break;
	case ORDER:
	  //	  Order::pages[idx-1].delete_flag = true;
	  break;
	case ORDERLINE:
	  //	  OrderLine::pages[idx-1].delete_flag = true;
	  break;
	case NEWORDER:
	  //	  NewOrder::pages[idx-1].delete_flag = true;
	  break;
	case ITEM:
	  break;
	case STOCK:
	  break;
	default:
	  PERR("switch");
	}
      }


      Log clog;
      memset(&clog,0,sizeof(Log));
      clog.type = COMPENSATION;
      clog.trans_id = log.trans_id;

      clog.page_id = log.page_id;
      clog.table_id = log.table_id;
      clog.undo_nxt_lsn = log.prev_lsn;
      clog.undo_nxt_offset = log.prev_offset;
      // clog.before isn't needed because compensation log record is redo-only.

      //      clog.after = log.before;

      clog.prev_lsn = dist_trans_table[th_id].LastLSN;
      clog.prev_offset = dist_trans_table[th_id].LastOffset;

      // compensation log recordをどこに書くかという問題はひとまず置いておく
      // とりあえず全部id=0のログブロックに書く
      ret = Logger::log_write(&clog, NULL, 0);

#ifdef DEBUG
      Logger::log_debug(clog);
#endif

      dist_trans_table[th_id].LastLSN = clog.lsn;
      dist_trans_table[th_id].LastOffset = clog.offset;
      dist_trans_table[th_id].UndoNxtLSN = clog.undo_nxt_lsn;
      dist_trans_table[th_id].UndoNxtOffset = clog.undo_nxt_offset;

      log_offset = log.prev_offset;
      continue;
    } else if(log.type == COMPENSATION){
      log_offset = log.undo_nxt_offset;
      continue;
    } else if(log.type == BEGIN){
      Log end_log;
      memset(&end_log,0,sizeof(Log));
      end_log.type = END;
      end_log.trans_id = log.trans_id;

      end_log.prev_lsn = dist_trans_table[th_id].LastLSN;
      end_log.prev_offset = dist_trans_table[th_id].LastOffset;
      end_log.undo_nxt_lsn = 0;
      end_log.undo_nxt_offset = 0;

      Logger::log_write(&end_log, NULL, 0);
#ifdef DEBUG
      Logger::log_debug(end_log);
#endif

      /* dist_transaction_tableのエントリを削除する. */
      memset(&dist_trans_table[th_id], 0, sizeof(DistributedTransTable));
    }
    log_offset = log.prev_offset;
  }

  close(log_fd);
}


