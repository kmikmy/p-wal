#ifndef _tpcc_table
#define _tpcc_table

#include <pthread.h>
#include <iostream>
#include "tpcc.h"
#include "tpcc_page.h"
#include "../../../include/ARIES.h"

/* #ifndef W */
/* #define W 1 // Scale Factor */
/* #endif */

extern DistributedTransTable *dist_trans_table;
extern int W;

class Constant{
public:
  int c_for_c_last;
  int c_for_c_id;
  int c_for_ol_i_id;

  Constant(){
    /* validate C-Run by using this value(C-Load) */
    int fd;
    int c_load, c_run, c_delta;
    if((fd = open(CLOADFILENAME, O_RDONLY)) == -1 ){
      PERR("open");
    }
    read(fd, &c_load, sizeof(int));
    close(fd);

    do{
      c_run = uniform(0, 255);
      c_delta = abs(c_run - c_load);
    }while( !(65 <= c_delta && c_delta <= 119 && c_delta != 96 && c_delta != 112) );

    c_for_c_last = c_run;
    c_for_c_id = uniform(0, 1023);
    c_for_ol_i_id = uniform(0, 8191);
  }
};

class Table{
};

class Warehouse : public Table{
 public:
  static PageWarehouse *pages;
  static uint32_t npage; // the number of all pages
  static uint32_t loaded_npage; // the number of loaded pages
  static uint32_t inserted_npage; // the number of inserted pages
  static pthread_mutex_t insert_lock;
  
  static TPCC_PAGE_LIST *first_appended_page;
  static TPCC_PAGE_LIST *last_appended_page;
  
  static pthread_rwlock_t *locks;
  static std::set<uint32_t> my_lock_table[32]; // worker毎に存在するロックテーブル

  static void
  init(){
    try {
      npage = W;
      loaded_npage = npage;
      pages = new PageWarehouse[npage]; 
      locks = new pthread_rwlock_t[npage];
      for(uint32_t i=0;i<npage;i++){
	pthread_rwlock_init(&locks[i], NULL);
      }
      pthread_mutex_init(&insert_lock,NULL);
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
  insert_list(TPCC_PAGE_LIST *plist){
    pthread_mutex_lock(&insert_lock);
    plist->page->page_id = ++npage;
    if(inserted_npage == 0){
      first_appended_page = plist;
      last_appended_page = first_appended_page;
    } else {
      last_appended_page->nxt = plist;
      plist->bfr = last_appended_page;
      last_appended_page = plist;
    }
    inserted_npage++;
    pthread_mutex_unlock(&insert_lock);
  }

  static int
  lock_page(int pageId, int thId, bool rlock_flag=false){
    int idx = pageId-1;
    int ret;

    /* rollbackは未実装 */
    if(my_lock_table[thId].find(pageId) == my_lock_table[thId].end()){
      /* 自スレッドが該当ページのロックをまだ獲得していない状態 */
      while(1){
	if(rlock_flag){
	  ret = pthread_rwlock_tryrdlock(&locks[idx]); // 読み込みロックの獲得を試みる
	  if(ret == 0) break; //success
	} else {
	  ret = pthread_rwlock_trywrlock(&locks[idx]); // 書き込みロックの獲得を試みる
	  if(ret == 0) break; //success
	}
      }
      //lockが完了したら、ロックテーブルに追加      
      my_lock_table[thId].insert(pageId);
    }

    else{ 
      // read_lockからwrite_lockへの昇格はまだ実装していない
      // upgrade_lockを使う必要がありそう
      ; //既に該当ページのロックを獲得している場合は何もしない
    }

    return 0;
  }

  static void
  unlock_table_page(int thId){
    std::set<uint32_t>::iterator it;
    for(it=my_lock_table[thId].begin(); it!=my_lock_table[thId].end();++it){
      pthread_rwlock_unlock(&locks[*it-1]);
    }
    my_lock_table[thId].clear();
  }

  static PageWarehouse*
  select1(uint32_t w_id, int thId){
    if(w_id <= npage){
      lock_page(pages[w_id-1].page_id, thId, true);
      return &pages[w_id-1];
    }
    else 
      return NULL;
  }
};

class District : public Table{
 public:
  static PageDistrict *pages;
  static uint32_t npage; // the number of all pages
  static uint32_t loaded_npage; // the number of loaded pages
  static uint32_t inserted_npage; // the number of inserted pages
  static pthread_mutex_t insert_lock;
  
  static TPCC_PAGE_LIST *first_appended_page;
  static TPCC_PAGE_LIST *last_appended_page;

  static pthread_rwlock_t *locks;
  static std::set<uint32_t> my_lock_table[32]; // worker毎に存在するロックテーブル

  static void
  init(){
    try {
      npage = W*10;
      loaded_npage = npage;
      pages = new PageDistrict[npage]; 
      locks = new pthread_rwlock_t[npage];
      for(uint32_t i=0;i<npage;i++){
	pthread_rwlock_init(&locks[i], NULL);
      }
      pthread_mutex_init(&insert_lock, NULL);
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
  insert_list(TPCC_PAGE_LIST *plist){
    pthread_mutex_lock(&insert_lock);
    plist->page->page_id = ++npage;
    if(inserted_npage == 0){
      first_appended_page = plist;
      last_appended_page = first_appended_page;
    } else {
      last_appended_page->nxt = plist;
      last_appended_page = plist;
    }
    inserted_npage++;
    pthread_mutex_unlock(&insert_lock);
  }

  static int
  lock_page(int pageId, int thId, bool rlock_flag=false){
    int idx = pageId-1;
    int ret;

    /* rollbackは未実装 */
    if(my_lock_table[thId].find(pageId) == my_lock_table[thId].end()){
      /* 自スレッドが該当ページのロックをまだ獲得していない状態 */
      while(1){
	if(rlock_flag){
	  //	  ret = pthread_rwlock_tryrdlock(&locks[idx]); // 読み込みロックの獲得を試みる
	  ret = pthread_rwlock_trywrlock(&locks[idx]); // 読み込んだ後に書き込む処理が有るため
	  if(ret == 0) break; //success
	} else {
	  ret = pthread_rwlock_trywrlock(&locks[idx]); // 書き込みロックの獲得を試みる
	  if(ret == 0) break; //success
	}
      }
      //lockが完了したら、ロックテーブルに追加      
      my_lock_table[thId].insert(pageId);
    }

    else{ 
      // read_lockからwrite_lockへの昇格はまだ実装していない
      // upgrade_lockを使う必要がありそう
      ; //既に該当ページのロックを獲得している場合は何もしない
    }

    return 0;
  }

  static void
  unlock_table_page(int thId){
    std::set<uint32_t>::iterator it;
    for(it=my_lock_table[thId].begin(); it!=my_lock_table[thId].end();++it){
      pthread_rwlock_unlock(&locks[*it-1]);
    }
    my_lock_table[thId].clear();
  }

  static PageDistrict*
  select1(uint32_t d_w_id, uint32_t d_id, uint32_t thId){
    return &pages[(d_w_id-1)*10+(d_id-1)];

    for(uint32_t i=0;i<npage;i++){
      if(pages[i].d_w_id == d_w_id && pages[i].d_id == d_id){
	lock_page(pages[i].page_id, thId);
	return &pages[i];
      }
    }
    return NULL;
  }

  static int
  update1(uint32_t d_next_o_id, PageDistrict* page, uint32_t thId, uint32_t xid){
    if(page == NULL)
      return 1;
    lock_page(page->page_id, thId);
    PageDistrict after;
    memcpy(&after, page, sizeof(PageDistrict));
    after.d_next_o_id = d_next_o_id;
    page->page_LSN = wal_write(xid, page, &after, thId);
    page->d_next_o_id = d_next_o_id;
    return 0;
  }

  static uint64_t
  wal_write(uint32_t xid, PageDistrict *before, PageDistrict *after, uint32_t thId){
    Log log;
    //   LSNはログを書き込む直前に決定する
    log.TransID = xid;
    if(before != NULL){
      log.Type = UPDATE;
    } else {
      log.Type = INSERT;
    }
    log.PageID = after->page_id;
    log.tid = DISTRICT;
    log.PrevLSN = dist_trans_table[thId].LastLSN;
    log.PrevOffset = dist_trans_table[thId].LastOffset;
    log.UndoNxtLSN = 0; // UndoNextLSNがログに書かれるのはCLRのみ.
    log.UndoNxtOffset = 0;

    if(before != NULL){
      memcpy(log.padding, before, sizeof(PageDistrict));
    }
    memcpy(log.padding+sizeof(PageDistrict), after, sizeof(PageDistrict));

    Logger::log_write(&log, thId);

    dist_trans_table[thId].LastLSN = log.LSN;
    dist_trans_table[thId].LastOffset = log.Offset;
    dist_trans_table[thId].UndoNxtLSN = log.LSN; // undoできる非CLRレコードの場合はUndoNxtLSNはLastLSNと同じになる
    dist_trans_table[thId].UndoNxtOffset = log.Offset;

    return log.LSN;
  }
};

class Customer : public Table{
 public:
  static PageCustomer *pages;
  static uint32_t npage; // the number of all pages
  static uint32_t loaded_npage; // the number of loaded pages
  static uint32_t inserted_npage; // the number of inserted pages
  static pthread_mutex_t insert_lock;
  
  static TPCC_PAGE_LIST *first_appended_page;
  static TPCC_PAGE_LIST *last_appended_page;

  static pthread_rwlock_t *locks;
  static std::set<uint32_t> my_lock_table[32]; // worker毎に存在するロックテーブル

  static void
  init(){
    try {
      npage = W*10*3000;
      loaded_npage = npage;
      pages = new PageCustomer[npage]; 
      locks = new pthread_rwlock_t[npage];
      for(uint32_t i=0;i<npage;i++){
	pthread_rwlock_init(&locks[i], NULL);
      }
      pthread_mutex_init(&insert_lock, NULL);
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
  insert_list(TPCC_PAGE_LIST *plist){
    pthread_mutex_lock(&insert_lock);
    plist->page->page_id = ++npage;
    if(inserted_npage == 0){
      first_appended_page = plist;
      last_appended_page = first_appended_page;
    } else {
      last_appended_page->nxt = plist;
      last_appended_page = plist;
    }
    inserted_npage++;
    pthread_mutex_unlock(&insert_lock);
  }

  static int
  lock_page(int pageId, int thId, bool rlock_flag=false){
    int idx = pageId-1;
    int ret;

    /* rollbackは未実装 */
    if(my_lock_table[thId].find(pageId) == my_lock_table[thId].end()){
      /* 自スレッドが該当ページのロックをまだ獲得していない状態 */
      while(1){
	if(rlock_flag){
	  ret = pthread_rwlock_tryrdlock(&locks[idx]); // 読み込みロックの獲得を試みる
	  if(ret == 0) break; //success
	} else {
	  ret = pthread_rwlock_trywrlock(&locks[idx]); // 書き込みロックの獲得を試みる
	  if(ret == 0) break; //success
	}
      }
      //lockが完了したら、ロックテーブルに追加      
      my_lock_table[thId].insert(pageId);
    }

    else{ 
      // read_lockからwrite_lockへの昇格はまだ実装していない
      // upgrade_lockを使う必要がありそう
      ; //既に該当ページのロックを獲得している場合は何もしない
    }

    return 0;
  }

  static void
  unlock_table_page(int thId){
    std::set<uint32_t>::iterator it;
    for(it=my_lock_table[thId].begin(); it!=my_lock_table[thId].end();++it){
      pthread_rwlock_unlock(&locks[(*it)-1]);
    }
    my_lock_table[thId].clear();
  }

  static PageCustomer*
  select1(uint32_t w_id, uint32_t d_id, uint32_t c_id, int thId){
    return &pages[((w_id-1)*10+(d_id-1))*3000+(c_id-1)];
    
    for(uint32_t i=0;i<npage;i++){
      if(pages[i].c_w_id == w_id && pages[i].c_d_id == d_id && pages[i].c_id == c_id){
	lock_page(pages[i].page_id, thId, true);
	return &pages[i];
      }
    }
    return NULL;
  }
};

class NewOrder : public Table{
public:
  static PageNewOrder *pages;
  static uint32_t npage; // the number of all pages
  static uint32_t loaded_npage; // the number of loaded pages
  static uint32_t inserted_npage; // the number of inserted pages
  static pthread_mutex_t insert_lock;
  
  static TPCC_PAGE_LIST *first_appended_page[MAX_WORKER_THREAD];
  static TPCC_PAGE_LIST *last_appended_page[MAX_WORKER_THREAD];

  static pthread_rwlock_t *locks;
  static std::set<uint32_t> my_lock_table[32]; // worker毎に存在するロックテーブル

  static void
  init(){
    try {
      npage = W*10*900;
      loaded_npage = npage;
      pages = new PageNewOrder[npage]; 
      locks = new pthread_rwlock_t[npage];
      for(uint32_t i=0;i<npage;i++){
	pthread_rwlock_init(&locks[i], NULL);
      }
      pthread_mutex_init(&insert_lock, NULL);
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
    insert_list(TPCC_PAGE_LIST *plist, int thId){
    //    pthread_mutex_lock(&insert_lock);
    plist->page->page_id = __sync_add_and_fetch(&npage,1);
    if(inserted_npage == 0){
      first_appended_page[thId] = plist;
      last_appended_page[thId] = plist;
    } else {
      last_appended_page[thId]->nxt = plist;
      last_appended_page[thId] = plist;
    }
    //    inserted_npage++;
    //    pthread_mutex_unlock(&insert_lock);
  }

  static void
  delete_list(uint32_t page_id, int thId){
    // insertされたテーブルが競合することは今のところ無いのでロックを取っていない
    if(page_id <= loaded_npage){
      pages[page_id-1].delete_flag = true;
    } else {
      TPCC_PAGE_LIST *p = first_appended_page[thId];
      while(p != NULL){
	if(p->page->page_id == page_id){
	  p->page->delete_flag = true;
	  break;
	}
	p = p->nxt;
      }
    }
  }

  static void
    insert1(PageNewOrder &_nop, int thId, uint32_t xid){
    TPCC_PAGE_LIST *plist = (TPCC_PAGE_LIST *)calloc(1, sizeof(TPCC_PAGE_LIST));
    PageNewOrder *nop = (PageNewOrder *)calloc(1, sizeof(PageNewOrder));
    memcpy(nop, &_nop, sizeof(PageNewOrder));
    plist->page = nop;
    insert_list(plist,thId);
    nop->page_LSN = wal_write(xid, NULL, (PageNewOrder *)plist->page, thId);
  }

  static uint64_t
  wal_write(uint32_t xid, PageNewOrder *before, PageNewOrder *after, int thId){
    Log log;

    //   LSNはログを書き込む直前に決定する
    log.TransID = xid;
    if(before != NULL){
      log.Type = UPDATE;
    } else {
      log.Type = INSERT;
    }
    log.PageID = after->page_id;
    log.tid = NEWORDER;
    log.PrevLSN = dist_trans_table[thId].LastLSN;
    log.PrevOffset = dist_trans_table[thId].LastOffset;
    log.UndoNxtLSN = 0; // UndoNextLSNがログに書かれるのはCLRのみ.
    log.UndoNxtOffset = 0;

    if(before != NULL){
      memcpy(log.padding, before, sizeof(PageNewOrder));
    }
    memcpy(log.padding+sizeof(PageNewOrder), after, sizeof(PageNewOrder));

    Logger::log_write(&log, thId);

    dist_trans_table[thId].LastLSN = log.LSN;
    dist_trans_table[thId].LastOffset = log.Offset;
    dist_trans_table[thId].UndoNxtLSN = log.LSN; // undoできる非CLRレコードの場合はUndoNxtLSNはLastLSNと同じになる
    dist_trans_table[thId].UndoNxtOffset = log.Offset;

    return log.LSN;
  }
};


class Order : public Table{
public:
  static PageOrder *pages;
  static uint32_t npage; // the number of all pages
  static uint32_t loaded_npage; // the number of loaded pages
  static uint32_t inserted_npage; // the number of inserted pages
  static pthread_mutex_t insert_lock;
  
  static TPCC_PAGE_LIST *first_appended_page[MAX_WORKER_THREAD];
  static TPCC_PAGE_LIST *last_appended_page[MAX_WORKER_THREAD];

  static pthread_rwlock_t *locks;
  static std::set<uint32_t> my_lock_table[32]; // worker毎に存在するロックテーブル

  static void
  init(){
    try {
      npage = W*10*3000;
      loaded_npage = npage;
      pages = new PageOrder[npage]; 
      locks = new pthread_rwlock_t[npage];
      for(uint32_t i=0;i<npage;i++){
	pthread_rwlock_init(&locks[i], NULL);
      }
      pthread_mutex_init(&insert_lock, NULL);
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
  insert_list(TPCC_PAGE_LIST *plist, int thId){
    //    pthread_mutex_lock(&insert_lock);
    plist->page->page_id = __sync_add_and_fetch(&npage,1);
    if(inserted_npage == 0){
      first_appended_page[thId] = plist;
      last_appended_page[thId] = plist;
    } else {
      last_appended_page[thId]->nxt = plist;
      last_appended_page[thId] = plist;
    }
    //    inserted_npage++;
    //    pthread_mutex_unlock(&insert_lock);
  }

  static void
  delete_list(uint32_t page_id, int thId){
    // insertされたテーブルが競合することは今のところ無いのでロックを取っていない
    if(page_id <= loaded_npage){
      pages[page_id-1].delete_flag = true;
    } else {
      TPCC_PAGE_LIST *p = first_appended_page[thId];
      while(p != NULL){
	if(p->page->page_id == page_id){
	  p->page->delete_flag = true;
	  break;
	}
	p = p->nxt;
      }
    }
  }

  static void
  insert1(PageOrder &_op, int thId, int xid){
    TPCC_PAGE_LIST *plist = (TPCC_PAGE_LIST *)calloc(1, sizeof(TPCC_PAGE_LIST));
    PageNewOrder *op = (PageNewOrder *)calloc(1, sizeof(PageOrder));
    memcpy(op, &_op, sizeof(PageOrder));
    plist->page = op;
    insert_list(plist,thId);
    op->page_LSN = wal_write(xid, NULL, (PageOrder *)plist->page, thId);
  }

  static uint64_t
  wal_write(uint32_t xid, PageOrder *before, PageOrder *after, int thId){
    Log log;
    //   LSNはログを書き込む直前に決定する
    log.TransID = xid;
    if(before != NULL){
      log.Type = UPDATE;
    } else {
      log.Type = INSERT;
    }
    log.PageID = after->page_id;
    log.tid = ORDER;
    log.PrevLSN = dist_trans_table[thId].LastLSN;
    log.PrevOffset = dist_trans_table[thId].LastOffset;
    log.UndoNxtLSN = 0; // UndoNextLSNがログに書かれるのはCLRのみ.
    log.UndoNxtOffset = 0;

    if(before != NULL){
      memcpy(log.padding, before, sizeof(PageOrder));
    }
    memcpy(log.padding+sizeof(PageOrder), after, sizeof(PageOrder));

    Logger::log_write(&log, thId);

    dist_trans_table[thId].LastLSN = log.LSN;
    dist_trans_table[thId].LastOffset = log.Offset;
    dist_trans_table[thId].UndoNxtLSN = log.LSN; // undoできる非CLRレコードの場合はUndoNxtLSNはLastLSNと同じになる
    dist_trans_table[thId].UndoNxtOffset = log.Offset;

    return log.LSN;
  }

};

class Item : public Table{
public:
  static uint32_t npage; // the number of all pages
  static uint32_t loaded_npage; // the number of loaded pages
  static uint32_t inserted_npage; // the number of inserted pages
  static pthread_mutex_t insert_lock;
  
  static TPCC_PAGE_LIST *first_appended_page;
  static TPCC_PAGE_LIST *last_appended_page;

  static pthread_rwlock_t *locks;
  static std::set<uint32_t> my_lock_table[32]; // worker毎に存在するロックテーブル

  static void
  init(){
    try {
      npage = 100000;
      loaded_npage = npage;
      pages = new PageItem[npage]; 
      locks = new pthread_rwlock_t[npage];
      for(uint32_t i=0;i<npage;i++){
	pthread_rwlock_init(&locks[i], NULL);
      }
      pthread_mutex_init(&insert_lock, NULL);
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
  insert_list(TPCC_PAGE_LIST *plist){
    pthread_mutex_lock(&insert_lock);
    plist->page->page_id = ++npage;
    if(inserted_npage == 0){
      first_appended_page = plist;
      last_appended_page = first_appended_page;
    } else {
      last_appended_page->nxt = plist;
      last_appended_page = plist;
    }
    inserted_npage++;
    pthread_mutex_unlock(&insert_lock);
  }

  static int
  lock_page(int pageId, int thId, bool rlock_flag=false){
    int idx = pageId-1;
    int ret;

    /* rollbackは未実装 */
    if(my_lock_table[thId].find(pageId) == my_lock_table[thId].end()){
      /* 自スレッドが該当ページのロックをまだ獲得していない状態 */
      while(1){
	if(rlock_flag){
	  ret = pthread_rwlock_tryrdlock(&locks[idx]); // 読み込みロックの獲得を試みる
	  if(ret == 0) break; //success
	} else {
	  ret = pthread_rwlock_trywrlock(&locks[idx]); // 書き込みロックの獲得を試みる
	  if(ret == 0) break; //success
	}
      }
      //lockが完了したら、ロックテーブルに追加      
      my_lock_table[thId].insert(pageId);
    } else{ 
      // read_lockからwrite_lockへの昇格はまだ実装していない
      // upgrade_lockを使う必要がありそう
      ; //既に該当ページのロックを獲得している場合は何もしない
    }

    return 0;
  }

  static void
  unlock_table_page(int thId){
    std::set<uint32_t>::iterator it;
    for(it=my_lock_table[thId].begin(); it!=my_lock_table[thId].end();++it){
      pthread_rwlock_unlock(&locks[*it-1]);
    }
    
    my_lock_table[thId].clear();
  }

  static PageItem*
    select1(uint32_t i_id, int thId){
    if(i_id <= npage){
      lock_page(pages[i_id-1].page_id, thId, true);
      return &pages[i_id-1];
    }
    return NULL;
  }

  static PageItem *pages;
};

class Stock : public Table{
 public:
  static PageStock *pages;
  static uint32_t npage; // the number of all pages
  static uint32_t loaded_npage; // the number of loaded pages
  static uint32_t inserted_npage; // the number of inserted pages
  static pthread_mutex_t insert_lock;
  
  static TPCC_PAGE_LIST *first_appended_page;
  static TPCC_PAGE_LIST *last_appended_page;

  static pthread_rwlock_t *locks;
  static std::set<uint32_t> my_lock_table[32]; // worker毎に存在するロックテーブル

  static void
  init(){
    try {
      npage = W*10*100000;
      loaded_npage = npage;
      pages = new PageStock[npage]; 
      locks = new pthread_rwlock_t[npage];
      for(uint32_t i=0;i<npage;i++){
	pthread_rwlock_init(&locks[i], NULL);
      }
      pthread_mutex_init(&insert_lock, NULL);
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
  insert_list(TPCC_PAGE_LIST *plist){
    pthread_mutex_lock(&insert_lock);
    plist->page->page_id = ++npage;
    if(inserted_npage == 0){
      first_appended_page = plist;
      last_appended_page = first_appended_page;
    } else {
      last_appended_page->nxt = plist;
      last_appended_page = plist;
    }
    inserted_npage++;
    pthread_mutex_unlock(&insert_lock);
  }

  static int
  lock_page(int pageId, int thId, bool rlock_flag=false){
    int idx = pageId-1;
    int ret;

    /* deadlockのrollbackは未実装 */
    if(my_lock_table[thId].find(pageId) == my_lock_table[thId].end()){
      /* 自スレッドが該当ページのロックをまだ獲得していない状態 */
      while(1){
	if(rlock_flag){
	  ret = pthread_rwlock_tryrdlock(&locks[idx]); // 読み込みロックの獲得を試みる
	  if(ret == 0) break; //success
	} else {
	  ret = pthread_rwlock_trywrlock(&locks[idx]); // 書き込みロックの獲得を試みる
	  if(ret == 0) break; //success
	}
      }
      //lockが完了したら、ロックテーブルに追加      
      my_lock_table[thId].insert(pageId);
    }

    else{ 
      // read_lockからwrite_lockへの昇格はまだ実装していない
      // upgrade_lockを使う必要がありそう
      ; //既に該当ページのロックを獲得している場合は何もしない
    }

    return 0;
  }

  static void
  unlock_table_page(int thId){
    std::set<uint32_t>::iterator it;
    for(it=my_lock_table[thId].begin(); it!=my_lock_table[thId].end();++it){
      pthread_rwlock_unlock(&locks[*it-1]);
    }
    my_lock_table[thId].clear();
  }

  static PageStock*
  select1(uint32_t s_i_id, uint32_t s_w_id, int thId){
    return &pages[(s_w_id-1)*100000+(s_i_id-1)];

    for(uint32_t i=0;i<npage;i++){
      if(pages[i].s_i_id == s_i_id && pages[i].s_w_id == s_w_id){
	lock_page(pages[i].page_id, thId, false);
	return &pages[i];
      }
    }
    return NULL;
  }

  static int
    update1(uint32_t s_quantity, uint32_t s_ytd, uint32_t s_order_cnt, uint32_t s_remote_cnt, PageStock *page, int thId, uint32_t xid){
    if(page == NULL){
      std::cout << "page is NULL" << std::endl;
      return 1;
    }
    lock_page(page->page_id, thId);
    PageStock before;
    memcpy(&before, page, sizeof(PageStock));
    page->s_quantity = s_quantity;
    page->s_ytd = s_ytd;
    page->s_order_cnt = s_order_cnt;
    page->s_remote_cnt = s_remote_cnt;
    page->page_LSN = wal_write(xid, &before, page, thId);
    return 0;
  }

  static uint64_t
  wal_write(uint32_t xid, PageStock *before, PageStock *after, int thId){
    Log log;
    //   LSNはログを書き込む直前に決定する
    log.TransID = xid;
    if(before != NULL){
      log.Type = UPDATE;
    } else {
      log.Type = INSERT;
    }
    log.PageID = after->page_id;
    log.tid = STOCK;
    log.PrevLSN = dist_trans_table[thId].LastLSN;
    log.PrevOffset = dist_trans_table[thId].LastOffset;
    log.UndoNxtLSN = 0; // UndoNextLSNがログに書かれるのはCLRのみ.
    log.UndoNxtOffset = 0;

    if(before != NULL){
      memcpy(log.padding, before, sizeof(PageStock));
    }
    memcpy(log.padding+sizeof(PageStock), after, sizeof(PageStock));

    Logger::log_write(&log, thId);

    dist_trans_table[thId].LastLSN = log.LSN;
    dist_trans_table[thId].LastOffset = log.Offset;
    dist_trans_table[thId].UndoNxtLSN = log.LSN; // undoできる非CLRレコードの場合はUndoNxtLSNはLastLSNと同じになる
    dist_trans_table[thId].UndoNxtOffset = log.Offset;

    return log.LSN;
  }
};

class OrderLine : public Table{
 public:
  static PageOrderLine *pages;
  static uint32_t npage; // the number of all pages
  static uint32_t loaded_npage; // the number of loaded pages
  static uint32_t inserted_npage; // the number of inserted pages
  static pthread_mutex_t insert_lock;
  
  static TPCC_PAGE_LIST *first_appended_page[MAX_WORKER_THREAD];
  static TPCC_PAGE_LIST *last_appended_page[MAX_WORKER_THREAD];

  static pthread_rwlock_t *locks;
  static std::set<uint32_t> my_lock_table[32]; // worker毎に存在するロックテーブル

  static void
  init(){
    try {
      npage = W*10*3000*15;
      loaded_npage = npage;
      pages = new PageOrderLine[npage]; 
      locks = new pthread_rwlock_t[npage];
      for(uint32_t i=0;i<npage;i++){
	pthread_rwlock_init(&locks[i], NULL);
      }
      pthread_mutex_init(&insert_lock, NULL);
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
  insert_list(TPCC_PAGE_LIST *plist, int thId){
    //    pthread_mutex_lock(&insert_lock);
    plist->page->page_id = __sync_add_and_fetch(&npage,1);
    if(inserted_npage == 0){
      first_appended_page[thId] = plist;
      last_appended_page[thId] = plist;
    } else {
      last_appended_page[thId]->nxt = plist;
      last_appended_page[thId] = plist;
    }
    //    inserted_npage++;
    //    pthread_mutex_unlock(&insert_lock);
  }

  static void
  delete_list(uint32_t page_id, int thId){
    // insertされたテーブルが競合することは今のところ無いのでロックを取っていない
    if(page_id <= loaded_npage){
      pages[page_id-1].delete_flag = true;
    } else {
      TPCC_PAGE_LIST *p = first_appended_page[thId];
      while(p != NULL){
	if(p->page->page_id == page_id){
	  p->page->delete_flag = true;
	  break;
	}
	p = p->nxt;
      }
    }
  }

  static void
    insert1(PageOrderLine &_olp, int thId, int xid){
    TPCC_PAGE_LIST *plist = (TPCC_PAGE_LIST *)calloc(1, sizeof(TPCC_PAGE_LIST));
    PageNewOrder *olp = (PageNewOrder *)calloc(1, sizeof(PageOrderLine));
    memcpy(olp, &_olp, sizeof(PageOrder));
    plist->page = olp;
    insert_list(plist, thId);
    olp->page_LSN = wal_write(xid, NULL, (PageOrderLine *)plist->page, thId);
  }

  static uint64_t
  wal_write(uint32_t xid, PageOrderLine *before, PageOrderLine *after, int thId){
    Log log;
    //   LSNはログを書き込む直前に決定する
    log.TransID = xid;
    if(before != NULL){
      log.Type = UPDATE;
    } else {
      log.Type = INSERT;
    }
    log.PageID = after->page_id;
    log.tid = ORDERLINE;
    log.PrevLSN = dist_trans_table[thId].LastLSN;
    log.PrevOffset = dist_trans_table[thId].LastOffset;
    log.UndoNxtLSN = 0; // UndoNextLSNがログに書かれるのはCLRのみ.
    log.UndoNxtOffset = 0;

    if(before != NULL){
      memcpy(log.padding, before, sizeof(PageOrderLine));
    }
    memcpy(log.padding+sizeof(PageOrderLine), after, sizeof(PageOrderLine));

    Logger::log_write(&log, thId);

    dist_trans_table[thId].LastLSN = log.LSN;
    dist_trans_table[thId].LastOffset = log.Offset;
    dist_trans_table[thId].UndoNxtLSN = log.LSN; // undoできる非CLRレコードの場合はUndoNxtLSNはLastLSNと同じになる
    dist_trans_table[thId].UndoNxtOffset = log.Offset;

    return log.LSN;
  }
};

#endif
