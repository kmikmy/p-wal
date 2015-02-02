#include "ARIES.h"
#include <iostream>
#include <cstdlib>
#include <ctime>
#include <map>
#include <set>
#include <sys/time.h>
#include <pthread.h>

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
static void rollback(uint32_t xid, int th_id);



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
  *page_id = rand() % PAGE_N;
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
  
  begin(xid, th_id);  // begin log write  
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
	if(pthread_rwlock_trywrlock(&pbuf->lock) == 0) break; // 書き込みロックの獲得に成功したらbreakする。
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
      int tmp = page_table[page_id].page.value;
    } else {
      WAL_update(op, xid, page_id, th_id);
    }
    // usleep(100);
  }
  end(xid, th_id); // end log write

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
  log.TransID = xid;
  log.Type = UPDATE;
  log.PageID = page_id;

  log.PrevLSN = dist_trans_table[th_id].LastLSN;
  log.PrevOffset = dist_trans_table[th_id].LastOffset;
  log.UndoNxtLSN = 0; // UndoNextLSNがログに書かれるのはCLRのみ.
  log.UndoNxtOffset = 0;
  log.op=op;
  log.before = pbuf->page.value;

  switch(op.op_type){
  case INC: pbuf->page.value += op.amount; break;
  case DEC: pbuf->page.value -= op.amount; break;
  case SUBST: pbuf->page.value = op.amount; break;
  default: break;
  }
  log.after = pbuf->page.value;

  //  ARIES_SYSTEM::transtable_debug();
  
  Logger::log_write(&log, th_id);
  pbuf->page.page_LSN = log.LSN;

  
  dist_trans_table[th_id].LastLSN = log.LSN;
  dist_trans_table[th_id].LastOffset = log.Offset;
  dist_trans_table[th_id].UndoNxtLSN = log.LSN; // undoできる非CLRレコードの場合はUndoNxtLSNはLastLSNと同じになる
  dist_trans_table[th_id].UndoNxtOffset = log.Offset;
  
  //  cout << "[Before Update]" << endl;
  //  cout << "page[" << p.page_id << "]: page_LSN=" << p.page_LSN << ", value=" << p.value << endl;

  /*  毎更新時にディスク上のページへforceしないのでコメントアウト */
  //  lseek(fd,sizeof(Page)*page_id,SEEK_SET);
  //  if( -1 == write(fd, &p, sizeof(Page))){
  //    perror("write"); exit(1);
  //  }    

  //  cout << "[After Update]" << endl;
  //  cout << "page[" << p.page_id << "]: page_LSN=" << p.page_LSN << ", value=" << p.value << endl;  
}

void 
begin(uint32_t xid, int th_id=0){
  Log log;
  memset(&log,0,sizeof(log));
  log.TransID = xid;
  log.Type = BEGIN;

  Logger::log_write(&log,th_id);

  dist_trans_table[th_id].TransID = xid;
  dist_trans_table[th_id].LastLSN = log.LSN;
  dist_trans_table[th_id].LastOffset = log.Offset;
  dist_trans_table[th_id].UndoNxtLSN = log.LSN; /* BEGIN レコードは END レコードによってコンペンセーションされる. */
  dist_trans_table[th_id].LastOffset = log.Offset;
  

#ifdef DEBUG
  Logger::log_debug(log);  
#endif
}

void 
end(uint32_t xid, int th_id=0){
  Log log;
  memset(&log,0,sizeof(log));
  log.TransID = xid;
  log.Type = END;
  log.PrevLSN = dist_trans_table[th_id].LastLSN;
  log.PrevOffset = dist_trans_table[th_id].LastOffset;
  log.UndoNxtLSN = 0; /* 一度 END ログが書かれたトランザクションは undo されることはない */
  log.UndoNxtOffset = 0; 

  Logger::log_write(&log,th_id);

  /* dist_transaction_tableのエントリを削除する. */
  memset(&dist_trans_table[th_id], 0, sizeof(DistributedTransTable));

#ifdef DEBUG
  Logger::log_debug(log);  
#endif
}


/*
  rollback()内ではトランザクションテーブルからエントリを削除しない.
  Iteratorを使って、トランザクションテーブルを巡回している場合があるため.
  rollback中のトランザクションテーブルの更新が不適(2014/11/14現在).
  recovery.cppのrollback_for_recoveryのコードをコピーする予定.
*/
void
rollback(uint32_t xid, int th_id){
  int log_fd = open(Logger::logpath, O_RDONLY);
  if(log_fd == -1){
    perror("open"); exit(1);
  }
  
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
    
    if (log.Type == UPDATE){
      int idx=log.PageID;
      
      page_table[idx].page.value = log.before;
      page_table[idx].page.page_LSN = log.PrevLSN; 

      Log clog;
      memset(&clog,0,sizeof(Log));
      clog.Type = COMPENSATION;
      clog.TransID = log.TransID;

      clog.PageID = log.PageID;
      clog.UndoNxtLSN = log.PrevLSN;
      clog.UndoNxtOffset = log.PrevOffset;
      // clog.before isn't needed because compensation log record is redo-only.
      clog.after = log.before;

      clog.PrevLSN = dist_trans_table[th_id].LastLSN;
      clog.PrevOffset = dist_trans_table[th_id].LastOffset;


      // compensation log recordをどこに書くかという問題はひとまず置いておく
      // とりあえず全部id=0のログブロックに書く
      ret = Logger::log_write(&clog, 0); 
      dist_trans_table[th_id].LastLSN = clog.LSN;
      dist_trans_table[th_id].LastOffset = clog.Offset;
      dist_trans_table[th_id].UndoNxtLSN = clog.UndoNxtLSN;
      dist_trans_table[th_id].UndoNxtOffset = clog.UndoNxtOffset;

#ifdef DEBUG
      Logger::log_debug(clog);
#endif

    }
    else if(log.Type == COMPENSATION){
      log_offset = log.UndoNxtOffset;
      continue;
    }
    else if(log.Type == BEGIN){
      Log end_log;
      memset(&end_log,0,sizeof(Log));
      end_log.Type = END;
      end_log.TransID = log.TransID;

      end_log.PrevLSN = dist_trans_table[th_id].LastLSN;
      end_log.PrevOffset = dist_trans_table[th_id].LastOffset;
      end_log.UndoNxtLSN = 0;
      end_log.UndoNxtOffset = 0;

      Logger::log_write(&end_log, 0);



#ifdef DEBUG
      Logger::log_debug(end_log);
#endif

      /* dist_transaction_tableのエントリを削除する. */
      memset(&dist_trans_table[th_id], 0, sizeof(DistributedTransTable));    
    }
    log_offset = log.Offset;
  }

  close(log_fd);
}


