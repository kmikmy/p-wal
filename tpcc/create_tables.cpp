#include "tpcc.h"
#include "tpcc_page.h"
#include "tpcc_util.h"
#include "debug.h"
#include <new>
#include <random>
#include <cstring>
#include <unistd.h>

void
create_warehouse(){
  int fd;
  if((fd = open(WFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1 ){
    PERR("open");
  }

  int cnt = W;
  PageWarehouse *pages;
  try {
    pages = new PageWarehouse[cnt]; 
  }
  catch(std::bad_alloc e) {
    PERR("new");
  }

  for(int w_id=1;w_id<=cnt;w_id++){
    pages[w_id-1].page_id = w_id;
    pages[w_id-1].w_id = w_id;
    gen_rand_astring(pages[w_id-1].w_name, 6, 10);
    gen_rand_astring(pages[w_id-1].w_street_1, 10, 20);
    gen_rand_astring(pages[w_id-1].w_street_2, 10, 20);
    gen_rand_astring(pages[w_id-1].w_city, 10, 20);
    gen_rand_nstring(pages[w_id-1].w_state, 2, 2);
    gen_rand_zip(pages[w_id-1].w_zip);
    gen_rand_decimal(&pages[w_id-1].w_tax, 0, 2000, 4);
    pages[w_id-1].w_ytd = 300000.00;
  }
  
  write(fd, pages, cnt*sizeof(PageWarehouse));
  delete[] pages;
  close(fd);

  std::cout << "create warehouse " << cnt << "records" << std::endl;
}

void create_district(){
  int fd,i=1;
  if((fd = open(DFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1 ){
    PERR("open");
  }

  int cnt = 10;
  PageDistrict *pages;
  try {
    pages = new PageDistrict[cnt]; 
  }
  catch(std::bad_alloc e) {
    PERR("new");
  }

  for(int w_id=1;w_id<=W;w_id++){
    for(int d_id=1;d_id<=cnt;d_id++){
      pages[d_id-1].page_id = i++;
      pages[d_id-1].d_id = d_id; // p-key
      pages[d_id-1].d_w_id = w_id; // p-key
      gen_rand_astring(pages[d_id-1].d_name, 6, 10);
      gen_rand_astring(pages[d_id-1].d_street_1, 10, 20);
      gen_rand_astring(pages[d_id-1].d_street_2, 10, 20);
      gen_rand_astring(pages[d_id-1].d_city, 10, 20);
      gen_rand_nstring(pages[d_id-1].d_state, 2, 2);
      gen_rand_zip(pages[d_id-1].d_zip);
      gen_rand_decimal(&pages[d_id-1].d_tax, 0, 2000, 4);
      pages[d_id-1].d_ytd = 30000.00;
      pages[d_id-1].d_next_o_id = 3001;
    }

    write(fd, pages, cnt*sizeof(PageDistrict));
  }

  delete[] pages;  
  std::cout << "create district " << cnt*W << "records" << std::endl;
  close(fd);
}

void create_customer(){
  int fd,i=1;
  int c_load = uniform(0, 255);

  /* validate C-Run by using this value(C-Load) */
  if((fd = open(CLOADFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1 ){
    PERR("open");
  }
  write(fd, &c_load, sizeof(c_load));
  close(fd);

  if((fd = open(CFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1 ){
    PERR("open");
  }

  int cnt = 3000;
  PageCustomer *pages;
  try {
    pages = new PageCustomer[cnt]; 
  }
  catch(std::bad_alloc e) {
    PERR("new");
  }

  for(int w_id=1;w_id<=W;w_id++){
    for(int d_id=1;d_id<=10;d_id++){
      for(int c_id=1;c_id<=cnt;c_id++){
	pages[c_id-1].page_id = i++;
	pages[c_id-1].c_id = c_id; // p-key
	pages[c_id-1].c_d_id = d_id; // p-key
	pages[c_id-1].c_w_id = w_id; // p-key
	if(c_id < 1000){
	  gen_c_last(pages[c_id-1].c_last, c_id);
	} else {
	  gen_c_last(pages[c_id-1].c_last, NURand(255, 0, 999, c_load));
	}
	strncpy(pages[c_id-1].c_middle, "OE", 3);
	gen_rand_astring(pages[c_id-1].c_first, 8, 16);
	gen_rand_astring(pages[c_id-1].c_street_1, 10, 20);
	gen_rand_astring(pages[c_id-1].c_street_2, 10, 20);
	gen_rand_astring(pages[c_id-1].c_city, 10, 20);
	gen_rand_astring(pages[c_id-1].c_state, 2, 2);
	gen_rand_zip(pages[c_id-1].c_zip);
	gen_rand_nstring(pages[c_id-1].c_phone, 16, 16);
	gen_date_and_time(pages[c_id-1].c_since);
	if(uniform(0,9)!=0){
	  strncpy(pages[c_id-1].c_credit, "GC", 3);
	} else {
	  strncpy(pages[c_id-1].c_credit, "BC", 3);
	}
	pages[c_id-1].c_credit_lim = 50000.00;
	gen_rand_decimal(&pages[c_id-1].c_discount, 0, 5000, 4);
	pages[c_id-1].c_balance = -10.00;
	pages[c_id-1].c_ytd_payment = 10.00;
	pages[c_id-1].c_payment_cnt = 1;
	pages[c_id-1].c_delivery_cnt = 0;
	gen_rand_astring(pages[c_id-1].c_data, 300, 500);
      }

      write(fd, pages, cnt*sizeof(PageCustomer));
    }
  }

  delete[] pages;
  close(fd);

  std::cout << "create customer " << cnt*10*W << "records" << std::endl;
}

void create_history(){
  int fd, i=1;
  if((fd = open(HFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1 ){
    PERR("open");
  }

  int cnt = 3000;
  PageHistory *pages;
  try {
    pages = new PageHistory[cnt]; 
  }
  catch(std::bad_alloc e) {
    PERR("new");
  }

  for(int w_id=1;w_id<=W;w_id++){
    for(int d_id=1;d_id<=10;d_id++){
      for(int c_id=1;c_id<=cnt;c_id++){
	pages[c_id-1].page_id = i++;
	pages[c_id-1].h_c_id = c_id;
	pages[c_id-1].h_c_d_id = d_id;
	pages[c_id-1].h_c_w_id = w_id;
	gen_date_and_time(pages[c_id-1].h_date);
	pages[c_id-1].h_amount = 10.00;
	gen_rand_astring(pages[c_id-1].h_data, 12, 24);
      }
      write(fd, pages, cnt*sizeof(PageHistory));
    }
  }
  delete[] pages;
  close(fd);

  std::cout << "create history " << cnt*10*W << "records" << std::endl;

}

void create_new_order(){
  int fd,i=1;

  if((fd = open(NOFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1 ){
    PERR("open");
  }

  int cnt = 900;
  PageNewOrder *pages;
  try {
    pages = new PageNewOrder[cnt]; 
  }
  catch(std::bad_alloc e) {
    PERR("new");
  }

  for(int w_id=1;w_id<=W;w_id++){
    for(int d_id=1;d_id<=10;d_id++){
      for(int o_id=1;o_id<=cnt;o_id++){
	pages[o_id-1].page_id = i++;
	pages[o_id-1].no_o_id = o_id+2100; // p-key
	pages[o_id-1].no_d_id = d_id; // p-key
	pages[o_id-1].no_w_id = w_id; // p-key
      }
      write(fd, pages, cnt*sizeof(PageNewOrder));
    }
  }
  
  delete[] pages;
  close(fd);

  std::cout << "create new-order " << W*10*cnt << "records" << std::endl;
  
}

void create_order(){
  int fd,ol_fd,i=1;

  if((fd = open(OFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1 ){
    PERR("open");
  }

  if((ol_fd = open(OLFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1 ){
    PERR("open");
  }

  int cnt = 3000;
  PageOrder *pages;
  PageOrderLine *ol_pages;
  try {
    pages = new PageOrder[cnt]; 
    ol_pages = new PageOrderLine[cnt]; 
  }
  catch(std::bad_alloc e) {
    PERR("new");
  }

  Permutation p(1,3000);

  int orderline_cnt=0;

  for(int w_id=1;w_id<=W;w_id++){
    for(int d_id=1;d_id<=10;d_id++){
      p.init(1,3000);
      for(int o_id=1;o_id<=cnt;o_id++){
	pages[o_id-1].page_id = i++;
	pages[o_id-1].o_id = o_id; // p-key
	pages[o_id-1].o_c_id = p.next();
	pages[o_id-1].o_d_id = d_id; // p-key
	pages[o_id-1].o_w_id = w_id; // p-key
	gen_date_and_time(pages[o_id-1].o_entry_d);
	if(o_id < 2101){
	  pages[o_id-1].o_carrier_id = uniform(1, 10); 
	} else {
	  pages[o_id-1].o_carrier_id = 0 ;  // 0 equal to null
	}
	pages[o_id-1].o_ol_cnt = uniform(5, 15);
	pages[o_id-1].o_all_local = 1;

	/* create_order_line() */
	static int ol_id=1;
	{
	  int cnt = pages[o_id-1].o_ol_cnt;
	  for(int ol_number=1;ol_number<=cnt;ol_number++){
	    orderline_cnt++;
	    ol_pages[ol_number-1].page_id = ol_id++;
	    ol_pages[ol_number-1].ol_o_id = o_id;
	    ol_pages[ol_number-1].ol_d_id = d_id;
	    ol_pages[ol_number-1].ol_w_id = w_id;
	    ol_pages[ol_number-1].ol_number = ol_number;
	    ol_pages[ol_number-1].ol_i_id = uniform(1, 100000);
	    ol_pages[ol_number-1].ol_supply_w_id = w_id;
	    if(o_id < 2101){
	      strncpy(ol_pages[ol_number-1].ol_delivery_d, pages[o_id-1].o_entry_d, sizeof(pages[o_id-1].o_entry_d));
	    } else {
	      ol_pages[ol_number-1].ol_delivery_d[0] = '\0';
	    }
	    ol_pages[ol_number-1].ol_quantity = 5;
	    if(o_id < 2101){	    
	      ol_pages[ol_number-1].ol_amount = 0.00;
	    } else {
	      gen_rand_decimal(&ol_pages[ol_number-1].ol_amount, 1, 999999, 2);	      
	    }
	    gen_rand_astring(ol_pages[o_id-1].ol_dist_info, 24, 24);
	  }
	  write(ol_fd, ol_pages, cnt*sizeof(PageOrderLine));
	} /* end of create_order_line */
      }

      write(fd, pages, cnt*sizeof(PageOrder));
    }
  }
  delete[] pages;
  delete[] ol_pages;
  close(ol_fd);
  close(fd);

  std::cout << "create order-line " << orderline_cnt << "records" << std::endl;
  std::cout << "create new-order " << W*10*cnt << "records" << std::endl;

}

void create_order_line(){
  /* included in create_order() */
}

void create_item(){
  int fd;
  if((fd = open(IFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1 ){
    PERR("open");
  }

  int cnt = 100000;
  PageItem *pages;
  try {
    pages = new PageItem[cnt]; 
  }
  catch(std::bad_alloc e) {
    PERR("new");
  }

  for(int i_id=1;i_id<=cnt;i_id++){
    pages[i_id-1].page_id = i_id;
    pages[i_id-1].i_id = i_id;
    pages[i_id-1].i_im_id = uniform(1,10000);
    gen_rand_astring(pages[i_id-1].i_name, 14, 24);
    gen_rand_decimal(&pages[i_id-1].i_price, 100, 10000, 2);
    gen_rand_astring_with_original(pages[i_id-1].i_data, 26, 50, 10);
    gen_rand_astring(pages[i_id-1].i_data, 26, 50);
  }

  write(fd, pages, cnt*sizeof(PageItem));
  delete[] pages;
  close(fd);

  std::cout << "create item " << cnt << "records" << std::endl;

}

void create_stock(){
  int fd, i=1;
  if((fd = open(SFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1 ){
    PERR("open");
  }

  int cnt = 100000;
  PageStock *pages;
  try {
    pages = new PageStock[cnt]; 
  }
  catch(std::bad_alloc e) {
    PERR("new");
  }

  for(int w=1;w<=W;w++){
    for(int s_i_id=1;s_i_id<=cnt;s_i_id++){
      pages[s_i_id-1].page_id = i++;
      pages[s_i_id-1].s_i_id = s_i_id;
      pages[s_i_id-1].s_w_id = w;
      pages[s_i_id-1].s_quantity = uniform(10, 100);
      gen_rand_astring(pages[s_i_id-1].s_dist_01, 24, 24);
      gen_rand_astring(pages[s_i_id-1].s_dist_02, 24, 24);
      gen_rand_astring(pages[s_i_id-1].s_dist_03, 24, 24);
      gen_rand_astring(pages[s_i_id-1].s_dist_04, 24, 24);
      gen_rand_astring(pages[s_i_id-1].s_dist_05, 24, 24);
      gen_rand_astring(pages[s_i_id-1].s_dist_06, 24, 24);
      gen_rand_astring(pages[s_i_id-1].s_dist_07, 24, 24);
      gen_rand_astring(pages[s_i_id-1].s_dist_08, 24, 24);
      gen_rand_astring(pages[s_i_id-1].s_dist_09, 24, 24);
      gen_rand_astring(pages[s_i_id-1].s_dist_10, 24, 24);
      pages[s_i_id-1].s_ytd = 0;
      pages[s_i_id-1].s_order_cnt = 0;
      pages[s_i_id-1].s_remote_cnt = 0;
      gen_rand_astring_with_original(pages[s_i_id-1].s_data, 26, 50, 10);
    }
    write(fd, pages, cnt*sizeof(PageStock));
  }

  delete[] pages;
  close(fd);

  std::cout << "create stock " << cnt * W<< "records" << std::endl;

}


void create_all(){
  create_warehouse();
  create_district();
  create_history();
  create_new_order();
  create_order();
  create_order_line();
  create_item();
  create_stock();
  create_customer();
}

int main(){
  create_all();
  return 0;
}
