#ifndef _tpcc_table
#define _tpcc_table

#include <pthread.h>
#include <iostream>
#include <memory>
#include "tpcc.h"
#include "tpcc_page.h"
#include "../../../include/log.h"
#include "../../../include/ARIES.h"

/* #ifndef W */
/* #define W 1 // Scale Factor */
/* #endif */

extern DistributedTransTable *dist_trans_table;

extern uint64_t update(const char* table_name, const std::vector<QueryArg> &qs, uint32_t page_id, uint32_t xid, uint32_t thId);
extern uint64_t insert(const char* table_name, const std::vector<QueryArg> &qs, uint32_t page_id, uint32_t xid, uint32_t thId);


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

    char fname[1][BUFSIZ] = {"d_next_o_id"};
    size_t fname_size[1];
    int  flen[1] = {sizeof(d_next_o_id)};
    char *fptr_before[1] = {(char *)&page->d_next_o_id};
    char *fptr_after[1] = {(char *)&d_next_o_id};

    QueryArg q;
    size_t alloc_size = 0;
    size_t ptr = 0;
    std::vector<QueryArg> qs(1);

    for(int i=0; i<1; i++){
      fname_size[i] = strlen(fname[i]) + 1;
      alloc_size += flen[i]*2 + fname_size[i];
    }


    char *data = (char *)calloc(1, alloc_size);
    if(data == NULL){ PERR("calloc"); }

    for(int i=0; i<1; i++){
      memcpy(data+ptr, fptr_before[i], flen[i]);
      memcpy(data+ptr+flen[i], fptr_after[i], flen[i]);
      memcpy(data+ptr+flen[i]*2, fname[i], fname_size[i]);

      q.before = data + ptr;
      q.after = data + ptr + flen[i];
      q.field_name = data + ptr + flen[i]*2;

      qs[i] = q;
      ptr += flen[i]*2 + fname_size[i];
    }

    page->page_LSN = ::update("district", qs, page->page_id, xid, thId);
    page->d_next_o_id = d_next_o_id;
    free(data);

    return 0;
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
  insert1(PageNewOrder &nop, int thId, uint32_t xid){
    TPCC_PAGE_LIST *plist = (TPCC_PAGE_LIST *)calloc(1, sizeof(TPCC_PAGE_LIST));
    PageNewOrder *page = (PageNewOrder *)calloc(1, sizeof(PageNewOrder));
    memcpy(page, &nop, sizeof(PageNewOrder));
    plist->page = page;

    char fname[3][BUFSIZ] = {"no_o_id", "no_d_id", "no_w_id"};
    size_t fname_size[3];
    int  flen[3] = {sizeof(nop.no_o_id), sizeof(nop.no_d_id), sizeof(nop.no_w_id)};
    char *fptr[3] = {(char *)&nop.no_o_id, (char *)&nop.no_d_id, (char *)&nop.no_w_id};

    QueryArg q;
    size_t alloc_size = 0;
    size_t ptr = 0;
    std::vector<QueryArg> qs(3);

    for(int i=0; i<3; i++){
      fname_size[i] = strlen(fname[i]) + 1;
      alloc_size += flen[i]*2 + fname_size[i];
    }

    char *data = (char *)calloc(1, alloc_size);
    if(data == NULL){ PERR("calloc"); }

    for(int i=0; i<3; i++){
      memcpy(data+ptr, fptr[i], flen[i]);
      memcpy(data+ptr+flen[i], fptr[i], flen[i]);
      memcpy(data+ptr+flen[i]*2, fname[i], fname_size[i]);

      q.before = data + ptr;
      q.after = data + ptr + flen[i];
      q.field_name = data + ptr + flen[i]*2;

      qs[i] = q;
      ptr += flen[i]*2 + fname_size[i];
    }

    page->page_LSN = ::insert("new_order", qs, page->page_id, xid, thId);
    free(data);

    insert_list(plist,thId);
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
  insert1(PageOrder &op, int thId, int xid){
    TPCC_PAGE_LIST *plist = (TPCC_PAGE_LIST *)calloc(1, sizeof(TPCC_PAGE_LIST));
    PageOrder *page = (PageOrder *)calloc(1, sizeof(PageOrder));
    memcpy(page, &op, sizeof(PageOrder));
    plist->page = page;

    char fname[8][BUFSIZ] = {"o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_carrier_id", "o_ol_cnt", "o_all_local"};
    size_t fname_size[8];
    int  flen[8] = {sizeof(op.o_id), sizeof(op.o_d_id), sizeof(op.o_w_id), sizeof(op.o_c_id), sizeof(op.o_entry_d), sizeof(op.o_carrier_id), sizeof(op.o_ol_cnt), sizeof(op.o_all_local)};
    char *fptr[8] = {(char *)&op.o_id, (char *)&op.o_d_id, (char *)&op.o_w_id, (char *)&op.o_c_id, (char *)&op.o_entry_d, (char *)&op.o_carrier_id, (char *)&op.o_ol_cnt, (char *)&op.o_all_local};

    QueryArg q;
    size_t alloc_size = 0;
    size_t ptr = 0;
    std::vector<QueryArg> qs(8);

    for(int i=0; i<8; i++){
      fname_size[i] = strlen(fname[i]) + 1;
      alloc_size += flen[i]*2 + fname_size[i];
    }

    char *data = (char *)calloc(1, alloc_size);
    if(data == NULL){ PERR("calloc"); }

    for(int i=0; i<8; i++){
      memcpy(data+ptr, fptr[i], flen[i]);
      memcpy(data+ptr+flen[i], fptr[i], flen[i]);
      memcpy(data+ptr+flen[i]*2, fname[i], fname_size[i]);

      q.before = data + ptr;
      q.after = data + ptr + flen[i];
      q.field_name = data + ptr + flen[i]*2;

      qs[i] = q;
      ptr += flen[i]*2 + fname_size[i];
    }

    page->page_LSN = ::insert("order", qs, page->page_id, xid, thId);
    free(data);

    insert_list(plist,thId);
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

    char fname[4][BUFSIZ] = {"s_quantity", "s_ytd", "s_order_cnt", "s_remote_cnt"};
    size_t fname_size[4];
    int  flen[4] = {sizeof(s_quantity), sizeof(s_ytd), sizeof(s_order_cnt), sizeof(s_remote_cnt)};
    char *fptr_before[4] = {(char *)&page->s_quantity, (char *)&page->s_ytd, (char *)&page->s_order_cnt, (char *)&page->s_remote_cnt};
    char *fptr_after[4] = {(char *)&s_quantity, (char *)&s_ytd, (char *)&s_order_cnt, (char *)&s_remote_cnt};

    QueryArg q;
    size_t alloc_size = 0;
    size_t ptr = 0;
    std::vector<QueryArg> qs(4);

    for(int i=0; i<4; i++){
      fname_size[i] = strlen(fname[i]) + 1;
      alloc_size += flen[i]*2 + fname_size[i];
    }


    char *data = (char *)calloc(1, alloc_size);
    if(data == NULL){ PERR("calloc"); }

    for(int i=0; i<4; i++){
      memcpy(data+ptr, fptr_before[i], flen[i]);
      memcpy(data+ptr+flen[i], fptr_after[i], flen[i]);
      memcpy(data+ptr+flen[i]*2, fname[i], fname_size[i]);

      q.before = data + ptr;
      q.after = data + ptr + flen[i];
      q.field_name = data + ptr + flen[i]*2;

      qs[i] = q;
      ptr += flen[i]*2 + fname_size[i];
    }

    page->page_LSN = update("stock", qs, page->page_id, xid, thId);
    free(data);

    page->s_quantity = s_quantity;
    page->s_ytd = s_ytd;
    page->s_order_cnt = s_order_cnt;
    page->s_remote_cnt = s_remote_cnt;

    return 0;
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
  insert1(PageOrderLine &olp, int thId, int xid){
    TPCC_PAGE_LIST *plist = (TPCC_PAGE_LIST *)calloc(1, sizeof(TPCC_PAGE_LIST));
    PageOrderLine *page = (PageOrderLine *)calloc(1, sizeof(PageOrderLine));
    memcpy(page, &olp, sizeof(PageOrderLine));
    plist->page = page;

    char fname[10][BUFSIZ] = {"ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount", "ol_dist_info"};
    size_t fname_size[10];
    int  flen[10] = {sizeof(olp.ol_o_id), sizeof(olp.ol_d_id), sizeof(olp.ol_w_id), sizeof(olp.ol_number), sizeof(olp.ol_i_id), sizeof(olp.ol_supply_w_id), sizeof(olp.ol_delivery_d), sizeof(olp.ol_quantity), sizeof(olp.ol_amount), sizeof(olp.ol_dist_info)};
    char *fptr[10] = {(char *)&olp.ol_o_id, (char *)&olp.ol_d_id, (char *)&olp.ol_w_id, (char *)&olp.ol_number, (char *)&olp.ol_i_id, (char *)&olp.ol_supply_w_id, (char *)&olp.ol_delivery_d, (char *)&olp.ol_quantity, (char *)&olp.ol_amount, (char *)&olp.ol_dist_info};

    QueryArg q;
    size_t alloc_size = 0;
    size_t ptr = 0;
    std::vector<QueryArg> qs(10);

    for(int i=0; i<10; i++){
      fname_size[i] = strlen(fname[i]) + 1;
      alloc_size += flen[i]*2 + fname_size[i];
    }

    char *data = (char *)calloc(1, alloc_size);
    if(data == NULL){ PERR("calloc"); }

    for(int i=0; i<10; i++){
      memcpy(data+ptr, fptr[i], flen[i]);
      memcpy(data+ptr+flen[i], fptr[i], flen[i]);
      memcpy(data+ptr+flen[i]*2, fname[i], fname_size[i]);

      q.before = data + ptr;
      q.after = data + ptr + flen[i];
      q.field_name = data + ptr + flen[i]*2;

      qs[i] = q;
      ptr += flen[i]*2 + fname_size[i];
    }

    page->page_LSN = ::insert("order_line", qs, page->page_id, xid, thId);
    free(data);

    insert_list(plist,thId);
  }
};

#endif
