#include "ARIES.h"
#include <iostream>
#include <cstdlib>
#include <ctime>
#include <map>
#include <set>
#include <sys/time.h>
#include <pthread.h>

#define EX1

using namespace std;

enum UP_OPTYPE { _EXIT, _INC, _DEC, _SUBST, _SYSTEM_FAILURE };

const uint32_t Delta = 500000; // Delta(ms)でロックが獲得できない場合はrollbackする。

extern TransTable trans_table;
extern PageBufferEntry pageBuffers[PAGE_N];
extern map<uint32_t, uint32_t> DPT;

extern void remove_transaction_xid(uint32_t xid);

void WAL_update(OP op, uint32_t xid, int pageID);
void begin_checkpoint();
void begin(uint32_t xid);
void end(uint32_t xid);
void rollback(uint32_t xid);



std::istream& 
operator>>( std::istream& is, UP_OPTYPE& i )
{
  int tmp ;
  if ( is >> tmp )
    i = static_cast<UP_OPTYPE>( tmp ) ;
  return is ;
}

static void
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

static void
page_select(uint32_t *page_id){
  *page_id = rand() % PAGE_N;
}

static void
lock_release(set<uint32_t> &lock_table){
  for(set<uint32_t>::iterator it = lock_table.begin(); it!=lock_table.end(); it++){
    pthread_rwlock_unlock(&pageBuffers[*it].lock);
  }
}


static int
process_transaction(uint32_t xid, OP *ops, uint32_t *page_ids, int update_num){
  set<uint32_t> my_lock_table;
  OP op;
  uint32_t pageID;

  struct timeval start_t, end_t;
  gettimeofday(&start_t, NULL);
  
  begin(xid);  // begin log write  
  for(int i=0;i<update_num;i++){
    op = ops[i];
    pageID = page_ids[i];

    PageBufferEntry *pbuf = &(pageBuffers[pageID]);
    if(my_lock_table.find(pageID) == my_lock_table.end()){ // 自スレッドが該当ページのロックをまだ獲得していない状態
      //      pthread_rwlock_init(&pbuf->lock, NULL);

      //      pthread_rwlock_wrlock(&pbuf->lock); //write lock
      
      // 開始時間の計測
      struct timeval s,t;
      gettimeofday(&s,NULL);
      while(1){
	if(pthread_rwlock_trywrlock(&pbuf->lock) == 0) break; //write lock
	// 開始時からの経過時間の計測（δ以上経過してたら)、打ち切ってロールバックする
	gettimeofday(&t,NULL);
	// deadlock detection
	if( ((t.tv_sec - s.tv_sec)*1000 + (t.tv_usec - s.tv_usec)/1000 ) > Delta ){
	  cout << "-" ;
	  rollback(xid);
	  remove_transaction_xid(xid);

	  lock_release(my_lock_table);
	  return -1;
	} 
	else{
	  //	  cout << "wait:" << xid << endl;
	}
      }
      
      //lockが完了したら、ロックテーブルに追加
      my_lock_table.insert(pageID);
    }
    else{ 
      ; //既に該当ページのロックを獲得している場合は何もしない
    }
    WAL_update(op, xid, pageID);
  }
  end(xid); // end log write

  gettimeofday(&end_t, NULL);

  // cout << xid << ":" << (end_t.tv_sec - start_t.tv_sec)*1000*1000 + end_t.tv_usec - start_t.tv_usec << "(us)" << endl;

  lock_release(my_lock_table);

  return 0;
}


/* 
   Tranasction id を渡して、Transactionを行う。
   この関数は、unfixed_thread_mode, fixed_thread_modeから共に呼び出される。
   実際にTransaction処理を行っているのは、process_transaction()。
   命令を構成したり、ロールバックした場合トランザクションを再実行するための
   ラッパ関数のようなもの。
 */
void *
th_transaction(void *_xid)  // _xid is uint32_t* type.
{
  uint32_t xid = *((uint32_t *)_xid);
  free(_xid); // メモリリークに気をつける

  /* 命令群を構成する */
  OP ops[MAX_UPDATE];
  uint32_t page_ids[MAX_UPDATE];
  int update_num = rand() % MAX_UPDATE + 1; // UPDATE 回数

#ifdef EX1
  update_num = 1;
#endif

  for(int i=0;i<update_num;i++){
    operation_select(&ops[i]);
    page_select(&page_ids[i]);
  }
  
  //  cout << "th_transaction: " << xid << endl;
  // transactionがrollbackしたら何度でも繰り返す
  while(process_transaction(xid, ops, page_ids, update_num) == -1);

  return NULL;
}


void WAL_update(OP op, uint32_t xid, int pageID){

  PageBufferEntry *pbuf = &pageBuffers[pageID];
  // fixed flagがなければファイルから読み込んでfixする

#ifdef DEBUG
    debug("fixed?: " << pbuf->fixed_flag << endl);
#endif

  if(!pbuf->fixed_flag){
    pbuf->fixed_flag = true;
    pbuf->modified_flag = true;

    int fd;
    if( (fd = open("/home/kamiya/hpcs/aries/data/pages.dat", O_CREAT | O_RDONLY )) == -1){
      perror("open");
      exit(1);
    }
    uint32_t log_end = lseek(fd, (off_t)sizeof(Page)*pageID, SEEK_SET);
    //    Page p;
    if( -1 == read(fd, &pbuf->page, sizeof(Page))){
      perror("read"); exit(1);
    } 
    pbuf->page_id = pageID;

    /* dirty_pages_tableのRecLSN更新(このLSNの後にディスク上ページに修正を加えていないログがあるかもしれないということを知るためのもの) */
    DPT[pbuf->page_id] = log_end;

    close(fd);
  }   

  Log log;
  //   LSNはログを書き込む直前に決定する
  log.TransID = xid;
  log.Type = UPDATE;
  log.PageID = pageID;
  log.PrevLSN = trans_table[xid].LastLSN;
  log.UndoNxtLSN = 0;
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

  Logger::log_write(&log);

  trans_table[xid].LastLSN = log.LSN; // Transaction tableの更新

  // write 
  //  cout << "[Before Update]" << endl;
  //  cout << "page[" << p.pageID << "]: page_LSN=" << p.page_LSN << ", value=" << p.value << endl;

  /*  毎更新時にディスク上のページへforceしないのでコメントアウト */
  //  p.page_LSN = log.LSN;
  //  lseek(fd,sizeof(Page)*pageID,SEEK_SET);
  //  if( -1 == write(fd, &p, sizeof(Page))){
  //    perror("write"); exit(1);
  //  }    

  //  cout << "[After Update]" << endl;
  //  cout << "page[" << p.pageID << "]: page_LSN=" << p.page_LSN << ", value=" << p.value << endl;  
}

void 
begin(uint32_t xid){
  Log log;
  memset(&log,0,sizeof(log));
  log.TransID = xid;
  log.Type = BEGIN;

  Logger::log_write(&log);
  trans_table[xid].LastLSN = log.LSN;
#ifdef DEBUG
  Logger::log_debug(log);  
#endif
}

void 
end(uint32_t xid){

  // if(rand()%3 == 0){ // 33%の確率でENDログがフラッシュされない
  //   cout << "end log wasn't written." << endl;
  //   return;
  // }

  Log log;
  memset(&log,0,sizeof(log));
  log.TransID = xid;
  log.Type = END;
  log.PrevLSN = trans_table[xid].LastLSN;

  Logger::log_write(&log);
  trans_table[xid].LastLSN = log.LSN;
#ifdef DEBUG
  Logger::log_debug(log);  
#endif
}


/*
  rollback()内ではトランザクションテーブルからエントリを削除しない。
  Iteratorを使って、トランザクションテーブルを巡回している場合があるため。
*/
void
rollback(uint32_t xid){
  int log_fd = open(Logger::logpath, O_RDONLY);
  if(log_fd == -1){
    perror("open"); exit(1);
  }

  LogHeader lh;  
  if( -1 == read(log_fd, &lh, sizeof(LogHeader))){
    perror("read"); exit(1);
  }

  uint32_t lsn = trans_table[xid].LastLSN; // rollbackするトランザクションの最後のLSN

  Log log;
  while(lsn != 0){ // lsnが0になるのはprevLSNが0のBEGINログを処理した後
    lseek(log_fd, lsn, SEEK_SET);

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
      
      pageBuffers[idx].page.value = log.before;
      //   pageBuffers[idx].page.page_LSN = log.LSN; 

      Log clog;
      memset(&clog,0,sizeof(Log));
      clog.Type = COMPENSATION;
      clog.TransID = log.TransID;
      clog.PrevLSN = trans_table[xid].LastLSN;
      clog.PageID = log.PageID;
      clog.UndoNxtLSN = log.PrevLSN;
      // clog.before isn't needed because compensation log record is redo-only.
      clog.after = log.before;


      ret = Logger::log_write(&clog);
      trans_table[xid].LastLSN = clog.LSN;

#ifdef DEBUG
      Logger::log_debug(clog);
#endif

    }
    else if(log.Type == COMPENSATION){
      lsn = log.UndoNxtLSN;
      continue;
    }
    else if(log.Type == BEGIN){
      Log clog;
      memset(&clog,0,sizeof(Log));
      clog.Type = COMPENSATION;
      clog.TransID = log.TransID;
      clog.PrevLSN = trans_table[xid].LastLSN;
      // clog.before isn't needed because compensation log record is redo-only.
      clog.UndoNxtLSN = log.PrevLSN;

      Logger::log_write(&clog);
      //      trans_table[xid].LastLSN = clog.LSN;
#ifdef DEBUG
      Logger::log_debug(clog);
#endif
      
    }
    lsn = log.PrevLSN;
  }

  close(log_fd);
}

