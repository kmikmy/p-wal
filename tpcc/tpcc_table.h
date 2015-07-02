#ifndef _tpcc_table
#define _tpcc_table

#include "../ARIES.h"
//#include "tpcc.h"
#include "tpcc_page.h"
#include "tpcc_table.h"
#include "tpcc_util.h"

#ifndef W
#define W 1 // Scale Factor
#endif


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

    srand(time(NULL));

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
 public:
 private:
 protected:
};

class Warehouse : public Table{
 public:
  static PageWarehouse *pages;
  static uint32_t npage; // the number of all pages
  static uint32_t loaded_npage; // the number of loaded pages
  static uint32_t inserted_npage; // the number of inserted pages
  
  static TPCC_PAGE_LIST *first_appended_page;
  static TPCC_PAGE_LIST *last_appended_page;

  static void
  init(){
    try {
      pages = new PageWarehouse[W]; 
      npage = W;
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
  insert_list(TPCC_PAGE_LIST *plist){
    if(inserted_npage == 0){
      first_appended_page = plist;
      last_appended_page = first_appended_page;
    } else {
      last_appended_page->nxt = plist;
      last_appended_page = plist;
    }
    inserted_npage++;
    npage++;
  }

  static PageWarehouse*
  select1(uint32_t w_id){
    if(w_id <= npage)
      return &pages[w_id-1];
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
  
  static TPCC_PAGE_LIST *first_appended_page;
  static TPCC_PAGE_LIST *last_appended_page;

  static void
  init(){
    try {
      pages = new PageDistrict[W*10]; 
      npage = W*10;
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
  insert_list(TPCC_PAGE_LIST *plist){
    if(inserted_npage == 0){
      first_appended_page = plist;
      last_appended_page = first_appended_page;
    } else {
      last_appended_page->nxt = plist;
      last_appended_page = plist;
    }
    inserted_npage++;
    npage++;
  }

  static PageDistrict*
  select1(uint32_t d_w_id, uint32_t d_id){
    for(uint32_t i=0;i<npage;i++){
      if(pages[i].d_w_id == d_w_id && pages[i].d_id == d_id)
	return &pages[i];
    }
    return NULL;
  }

  static int
  update1(uint32_t d_next_o_id, PageDistrict* page){
    if(page == NULL)
      return 1;
    page->d_next_o_id = d_next_o_id;
    return 0;
  }
};

class Customer : public Table{
 public:
  static PageCustomer *pages;
  static uint32_t npage; // the number of all pages
  static uint32_t loaded_npage; // the number of loaded pages
  static uint32_t inserted_npage; // the number of inserted pages
  
  static TPCC_PAGE_LIST *first_appended_page;
  static TPCC_PAGE_LIST *last_appended_page;

  static void
  init(){
    try {
      pages = new PageCustomer[W*10*3000]; 
      npage = W*10*3000;
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
  insert_list(TPCC_PAGE_LIST *plist){
    if(inserted_npage == 0){
      first_appended_page = plist;
      last_appended_page = first_appended_page;
    } else {
      last_appended_page->nxt = plist;
      last_appended_page = plist;
    }
    inserted_npage++;
    npage++;
  }

  static PageCustomer*
  select1(uint32_t w_id, uint32_t d_id, uint32_t c_id){
    for(uint32_t i=0;i<npage;i++){
      if(pages[i].c_w_id == w_id && pages[i].c_d_id == d_id && pages[i].c_id == c_id)
	return &pages[i];
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
  
  static TPCC_PAGE_LIST *first_appended_page;
  static TPCC_PAGE_LIST *last_appended_page;

  static void
  init(){
    try {
      pages = new PageNewOrder[W*10*900]; 
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
  insert_list(TPCC_PAGE_LIST *plist){
    if(inserted_npage == 0){
      first_appended_page = plist;
      last_appended_page = first_appended_page;
    } else {
      last_appended_page->nxt = plist;
      last_appended_page = plist;
    }
    inserted_npage++;
    npage++;
  }

  static void
  insert1(PageNewOrder &_nop){
    TPCC_PAGE_LIST *plist = (TPCC_PAGE_LIST *)calloc(1, sizeof(TPCC_PAGE_LIST));
    PageNewOrder nop;
    memcpy(&nop, &_nop, sizeof(PageNewOrder));
    plist->page = nop;
    insert_list(plist);
  }
};


class Order : public Table{
public:
  static PageOrder *pages;
  static uint32_t npage; // the number of all pages
  static uint32_t loaded_npage; // the number of loaded pages
  static uint32_t inserted_npage; // the number of inserted pages
  
  static TPCC_PAGE_LIST *first_appended_page;
  static TPCC_PAGE_LIST *last_appended_page;

  static void
  init(){
    try {
      pages = new PageOrder[W*10*3000]; 
      npage = W*10*3000;
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
  insert_list(TPCC_PAGE_LIST *plist){
    if(inserted_npage == 0){
      first_appended_page = plist;
      last_appended_page = first_appended_page;
    } else {
      last_appended_page->nxt = plist;
      last_appended_page = plist;
    }
    inserted_npage++;
    npage++;
  }

  static void
  insert1(PageOrder &_op){
    TPCC_PAGE_LIST *plist = (TPCC_PAGE_LIST *)calloc(1, sizeof(TPCC_PAGE_LIST));
    PageOrder op;
    memcpy(&op, &_op, sizeof(PageOrder));
    plist->page = op;
    insert_list(plist);
  }
};

class Item : public Table{
public:
  static uint32_t npage; // the number of all pages
  static uint32_t loaded_npage; // the number of loaded pages
  static uint32_t inserted_npage; // the number of inserted pages
  
  static TPCC_PAGE_LIST *first_appended_page;
  static TPCC_PAGE_LIST *last_appended_page;

  static void
  init(){
    try {
      pages = new PageItem[100000]; 
      npage = 100000;
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
  insert_list(TPCC_PAGE_LIST *plist){
    if(inserted_npage == 0){
      first_appended_page = plist;
      last_appended_page = first_appended_page;
    } else {
      last_appended_page->nxt = plist;
      last_appended_page = plist;
    }
    inserted_npage++;
    npage++;
  }

  static PageItem*
  select1(uint32_t i_id){
    if(i_id <= npage){
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
  
  static TPCC_PAGE_LIST *first_appended_page;
  static TPCC_PAGE_LIST *last_appended_page;

  static void
  init(){
    try {
      pages = new PageStock[W*10*100000]; 
      npage = W*10*100000;
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
  insert_list(TPCC_PAGE_LIST *plist){
    if(inserted_npage == 0){
      first_appended_page = plist;
      last_appended_page = first_appended_page;
    } else {
      last_appended_page->nxt = plist;
      last_appended_page = plist;
    }
    inserted_npage++;
    npage++;
  }

  static PageStock*
  select1(uint32_t s_i_id, uint32_t s_w_id){
    for(uint32_t i=0;i<npage;i++){
      if(pages[i].s_i_id == s_i_id && pages[i].s_w_id == s_w_id)
	return &pages[i];
    }
    return NULL;
  }

  static int
    update1(uint32_t s_quantity, uint32_t s_ytd, uint32_t s_order_cnt, uint32_t s_remote_cnt, PageStock *page){
    if(page == NULL)
      return 1;
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
  
  static TPCC_PAGE_LIST *first_appended_page;
  static TPCC_PAGE_LIST *last_appended_page;

  static void
  init(){
    try {
      pages = new PageOrderLine[W*10*3000*15]; 
      npage = W*10*3000*15;
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }
  }

  static void
  insert_list(TPCC_PAGE_LIST *plist){
    if(inserted_npage == 0){
      first_appended_page = plist;
      last_appended_page = first_appended_page;
    } else {
      last_appended_page->nxt = plist;
      last_appended_page = plist;
    }
    inserted_npage++;
    npage++;
  }

  static void
  insert1(PageOrderLine &olp){
    //   TPCC_PAGE_LIST *plist = (TPCC_PAGE_LIST *)calloc(1, sizeof(TPCC_PAGE_LIST));
    //    if(plist == NULL){
    //      PERR("calloc");
    //    }
    //      
    //    memcpy(&plist->page, &olp, sizeof(PageOrderLine));
    //    insert_list(plist);
  }
};

#endif
