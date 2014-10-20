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

const uint32_t Delta = 500; // Delta(ms)でロックが獲得できない場合はrollbackする。

extern TransTable trans_table;
/* trans_tableを変更する際にはロックが必要 */
extern std::mutex trans_table_mutex;

extern PageBufferEntry page_table[PAGE_N];
extern map<uint32_t, uint32_t> dirty_page_table;
extern char *ARIES_HOME;

extern void remove_transaction_xid(uint32_t xid);
extern void WAL_update(OP op, uint32_t xid, int page_id, int th_id);

void begin_checkpoint();
void begin(uint32_t xid, int th_id);
void end(uint32_t xid, int th_id);
void rollback(uint32_t xid);


static int page_fd;
static std::mutex page_mtx;

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

    PageBufferEntry *pbuf = &(page_table[page_id]);

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
      my_lock_table.insert(page_id);
    }
    else{ 
      ; //既に該当ページのロックを獲得している場合は何もしない
    }
    WAL_update(op, xid, page_id, th_id);
    // usleep(100);
  }
  end(xid, th_id); // end log write

  lock_release(my_lock_table);

  return 0;
}

void WAL_update(OP op, uint32_t xid, int page_id, int th_id){
  PageBufferEntry *pbuf = &page_table[page_id];
  // fixed flagがなければファイルから読み込んでfixする

#ifdef DEBUG
    debug("fixed?: " << pbuf->fixed_flag << endl);
#endif

  if(!pbuf->fixed_flag){
    pbuf->fixed_flag = true;
    pbuf->modified_flag = true;

    std::lock_guard<std::mutex> lock(page_mtx);
    std::string page_filename = ARIES_HOME;
    page_filename += "/data/pages.dat";

    if( page_fd == 0 ){
      if( (page_fd = open(page_filename.c_str(), O_CREAT | O_RDONLY )) == -1){
	perror("open");
	exit(1);
      }
    }
    lseek(page_fd, (off_t)sizeof(Page)*page_id, SEEK_SET);


    //    Page p;
    if( -1 == read(page_fd, &pbuf->page, sizeof(Page))){
      perror("read"); exit(1);
    } 
    pbuf->page_id = page_id;

    uint32_t rec_LSN = pbuf->page.page_LSN;

    /* 
       dirty_pages_tableのRecLSN更新
       RecLSNはそのページの更新が確実に反映されているログのLSN。
       リカバリ時にはそれ以降のLSNについてログからupdate内容を適用しなおさなければいけない。
    */
    dirty_page_table[pbuf->page_id] = rec_LSN;

  }   

  Log log;
  //   LSNはログを書き込む直前に決定する
  log.TransID = xid;
  log.Type = UPDATE;
  log.PageID = page_id;

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
  
  //  std::lock_guard<std::mutex> lock(trans_table_mutex);
  //  log.PrevLSN = trans_table.at(xid).LastLSN;

  Logger::log_write(&log, th_id);
  //  trans_table[xid].LastLSN = log.LSN; // Transaction tableの更新


  // write 
  //  cout << "[Before Update]" << endl;
  //  cout << "page[" << p.page_id << "]: page_LSN=" << p.page_LSN << ", value=" << p.value << endl;

  /*  毎更新時にディスク上のページへforceしないのでコメントアウト */
  //  p.page_LSN = log.LSN;
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

  //   std::lock_guard<std::mutex> lock(trans_table_mutex);
  //   trans_table[xid].LastLSN = log.LSN;
#ifdef DEBUG
  Logger::log_debug(log);  
#endif
}

void 
end(uint32_t xid, int th_id=0){

  // if(rand()%3 == 0){ // 33%の確率でENDログがフラッシュされない
  //   cout << "end log wasn't written." << endl;
  //   return;
  // }

  Log log;
  memset(&log,0,sizeof(log));
  log.TransID = xid;
  log.Type = END;
  
  //  std::lock_guard<std::mutex> lock(trans_table_mutex);
  //  log.PrevLSN = trans_table.at(xid).LastLSN;

  Logger::log_write(&log,th_id);

  //  trans_table[xid].LastLSN = log.LSN;
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
  
  uint32_t lsn = trans_table.at(xid).LastLSN; // rollbackするトランザクションの最後のLSN

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
      
      page_table[idx].page.value = log.before;
      //   page_table[idx].page.page_LSN = log.LSN; 

      Log clog;
      memset(&clog,0,sizeof(Log));
      clog.Type = COMPENSATION;
      clog.TransID = log.TransID;

      clog.PageID = log.PageID;
      clog.UndoNxtLSN = log.PrevLSN;
      // clog.before isn't needed because compensation log record is redo-only.
      clog.after = log.before;

      // std::lock_guard<std::mutex> lock(trans_table_mutex);
      // clog.PrevLSN = trans_table.at(xid).LastLSN;


      // compensation log recordをどこに書くかという問題はひとまず置いておく
      // とりあえず全部同じログブロックに書く
      ret = Logger::log_write(&clog, 0); 
      // trans_table[xid].LastLSN = clog.LSN;

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
      // clog.before isn't needed because compensation log record is redo-only.
      clog.UndoNxtLSN = log.PrevLSN;

      // std::lock_guard<std::mutex> lock(trans_table_mutex);
      // clog.PrevLSN = trans_table.at(xid).LastLSN;

      Logger::log_write(&clog, 0);
      // compensation logはcompensateされないのでtransaction tableに記録する必要はない
      //      trans_table[xid].LastLSN = clog.LSN;
#ifdef DEBUG
      Logger::log_debug(clog);
#endif
      
    }
    lsn = log.PrevLSN;
  }

  close(log_fd);
}


void 
flush_page(){

  std::lock_guard<std::mutex> lock(page_mtx);  
  
  for(int i=0;i<PAGE_N;i++){
    if(!page_table[i].fixed_flag)
      continue;

    lseek(page_fd,sizeof(Page)*page_table[i].page_id, SEEK_SET);
    if( -1 == write(page_fd, &page_table[i].page, sizeof(Page))){
      perror("write"); exit(1);
    }
  }    
}
