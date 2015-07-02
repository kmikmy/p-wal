/*
 * テーブルの定義はcreate_tableとload_tableに使われている。
 * new-order.cppではtpcc_table.hを参照しているのでテーブルの定義は使われていないが、
 * スケールファクターはここで定義しているので読み込む必要がある。
 */

#ifndef _tpc
#define _tpc

#include <string>
#include <iostream>
#include <cstdint>
#include <new>
#include <vector>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "../ARIES.h"
#include "debug.h"
#include "tpcc_page.h"

#ifndef W
#define W 1 // Scale Factor
#endif

#define INIT_READ_PAGE 1000

enum TPCC_TABLE_TYPE{ WAREHOUSE, DISTRICT, CUSTOMER, HISTORY, NEW_ORDER, ORDER, ORDER_LINE, ITEM, STOCK };

class TPCC_TABLE{
 public:
  TPCC_TABLE_TYPE table_type;
  int page_cnt;
  TPCC_PAGE *pages;
  
  template <class T>int select(std::string column, T val);
  template <class T>void update(int key, std::string column, T val);

  void read_file(const char *fname, TPCC_PAGE *pages, size_t size){
    int fd, readbytes;
    if((fd = open("fname", O_RDONLY)) == -1){
      PERR("open");
    }

    while(1){
      readbytes = read(fd, pages, size*INIT_READ_PAGE);
      if(readbytes == -1){
	PERR("open");
      } else if(readbytes == 0){
	break;
      }
    }
  };
};

template <class T>
int TPCC_TABLE::select(std::string column, T val){
  std::cout << "select" << std::endl;
  return 1;
}

template <class T>
void TPCC_TABLE::update(int key,std::string column, T val){
  std::cout << "update" << std::endl;
}


class TableWarehouse : public TPCC_TABLE{
 public:
  TableWarehouse(){ init(); }
  ~TableWarehouse(){ term(); }

  void init(){
    page_cnt = W;
    try {
      pages = new PageWarehouse[W]; 
    }
    catch(std::bad_alloc &) {
      delete[] pages;
    }
    
    read_file("Warehouse.dat", pages, sizeof(PageWarehouse));
  }

  void term(){
    delete[] pages;
  }

  
};

class TableDistrict : public TPCC_TABLE{
 public:
  TableDistrict(){
    pages = new PageDistrict[10*W];
  }
};

class TableCustomer : public TPCC_TABLE{
 public:
  TableCustomer(){
    pages = new PageCustomer[30000*W];
  }
};

class TableHistory : public TPCC_TABLE{
 public:
  TableHistory(){ init(); }
  ~TableHistory(){ term(); }

  void init(){
    page_cnt = 30000*W;
    try {
      pages = new PageHistory[30000*W]; // +α
    }
    catch(std::bad_alloc &) {
      delete[] pages;
    }
    
  }

  void term(){
  }

  void construct_raw(int i){
    pages[i].page_id = i;
  }
};


class TableNewOrder : public TPCC_TABLE{
 public:
  TableNewOrder(){
    pages = new PageNewOrder[9000*W]; // +α
  }
};

class TableOrder : public TPCC_TABLE{
 public:
  TableOrder(){
    pages = new PageOrder[30000*W]; // +α
  }
};

class TableOrderLine : public TPCC_TABLE{
 public:
  TableOrderLine(){
    pages = new PageOrderLine[300000*W]; // +α
  }
};

class TableItem : public TPCC_TABLE{
 public:
  TableItem(){
    pages = new PageItem[100000];
  }
};

class TableStock : public TPCC_TABLE{
 public:
  TableStock(){
    pages = new PageStock[100000*W];
  }
};






#endif
