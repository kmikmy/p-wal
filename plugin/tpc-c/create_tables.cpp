#include <new>
#include <random>
#include <cstring>
#include <unistd.h>
#include <vector>
#include <exception>
#include "../../include/util.h"
#include "../../include/cmdline.h"
#include "include/tpcc.h"
#include "include/tpcc_page.h"
#include "include/tpcc_util.h"
#include "include/debug.h"

void
create_warehouse(){
  FD fd;
  std::vector<PageWarehouse> pages(W);

  for(int i=0; i<W; i++){
    const int w_id = i+1;
    pages[i].page_id = w_id;
    pages[i].w_id = w_id;
    gen_rand_astring(pages[i].w_name, 6, 10);
    gen_rand_astring(pages[i].w_street_1, 10, 20);
    gen_rand_astring(pages[i].w_street_2, 10, 20);
    gen_rand_astring(pages[i].w_city, 10, 20);
    gen_rand_nstring(pages[i].w_state, 2, 2);
    gen_rand_zip(pages[i].w_zip);
    gen_rand_decimal(&pages[i].w_tax, 0, 2000, 4);
    pages[i].w_ytd = 300000.00;
  }

  fd.open(WFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  fd.write(&pages[0], pages.size()*sizeof(PageWarehouse));

  std::cerr << "create warehouse " << pages.size() << "records: " << WFILENAME << std::endl;
}

void create_district(){
  FD fd;
  std::vector<PageDistrict> pages(10*W);

  for(int i=0; i<W; i++){
    for(int j=0; j<10; j++){
      int idx = 10*i+j;

      pages[idx].page_id = idx+1;
      pages[idx].d_id = i+i; // p-key
      pages[idx].d_w_id = j+1; // p-key
      gen_rand_astring(pages[idx].d_name, 6, 10);
      gen_rand_astring(pages[idx].d_street_1, 10, 20);
      gen_rand_astring(pages[idx].d_street_2, 10, 20);
      gen_rand_astring(pages[idx].d_city, 10, 20);
      gen_rand_nstring(pages[idx].d_state, 2, 2);
      gen_rand_zip(pages[idx].d_zip);
      gen_rand_decimal(&pages[idx].d_tax, 0, 2000, 4);
      pages[idx].d_ytd = 30000.00;
      pages[idx].d_next_o_id = 3001;
    }
  }

  fd.open(DFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  fd.write(&pages[0], pages.size()*sizeof(PageDistrict));

  std::cerr << "create district " << pages.size() << "records: " << DFILENAME << std::endl;
}

void create_customer(){
  FD fd;
  int c_load = uniform(0, 255);

  /* validate C-Run by using this value(C-Load) */
  fd.open(CLOADFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  fd.write(&c_load, sizeof(c_load));
  fd.close();

  int n = W*10*3000;
  std::vector<PageCustomer> pages(n);

  for(int i=0; i<W; i++){
    for(int j=0; j<10; j++){
      for(int k=0; k<3000; k++){
	int idx = i*10*3000 + j*3000 + k;

	pages[idx].page_id = idx+1;
	pages[idx].c_id   = k+1; // p-key
	pages[idx].c_d_id = j+1; // p-key
	pages[idx].c_w_id = i+1; // p-key
	if(pages[idx].c_id < 1000){
	  gen_c_last(pages[idx].c_last, pages[idx].c_id);
	} else {
	  gen_c_last(pages[idx].c_last, NURand(255, 0, 999, c_load));
	}
	strncpy(pages[idx].c_middle, "OE", 3);
	gen_rand_astring(pages[idx].c_first, 8, 16);
	gen_rand_astring(pages[idx].c_street_1, 10, 20);
	gen_rand_astring(pages[idx].c_street_2, 10, 20);
	gen_rand_astring(pages[idx].c_city, 10, 20);
	gen_rand_astring(pages[idx].c_state, 2, 2);
	gen_rand_zip(pages[idx].c_zip);
	gen_rand_nstring(pages[idx].c_phone, 16, 16);
	gen_date_and_time(pages[idx].c_since);
	if(uniform(0,9)!=0){
	  strncpy(pages[idx].c_credit, "GC", 3);
	} else {
	  strncpy(pages[idx].c_credit, "BC", 3);
	}
	pages[idx].c_credit_lim = 50000.00;
	gen_rand_decimal(&pages[idx].c_discount, 0, 5000, 4);
	pages[idx].c_balance = -10.00;
	pages[idx].c_ytd_payment = 10.00;
	pages[idx].c_payment_cnt = 1;
	pages[idx].c_delivery_cnt = 0;
	gen_rand_astring(pages[idx].c_data, 300, 500);
      }
    }
  }

  fd.open(CFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  fd.write(&pages[0], pages.size()*sizeof(PageCustomer));

  std::cerr << "create customer " << pages.size() << "records: " << CFILENAME << std::endl;
}

void create_history(){
  FD fd;
  std::vector<PageHistory> pages(W*10*3000);

  for(int i=0; i<W; i++){
    for(int j=0; j<10; j++){
      for(int k=0; k<3000; k++){
	int idx = i*10*3000 + j*3000 + k;

	pages[idx].page_id = i;
	pages[idx].h_c_w_id = i+1;
	pages[idx].h_c_d_id = j+1;
	pages[idx].h_c_id = k+1;

	gen_date_and_time(pages[idx].h_date);
	pages[idx].h_amount = 10.00;
	gen_rand_astring(pages[idx].h_data, 12, 24);
      }
    }
  }

  fd.open(HFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  fd.write(&pages[0], pages.size()*sizeof(PageHistory));

  std::cerr << "create history " << pages.size() << "records: " << HFILENAME << std::endl;
}

static void create_order_line(std::vector<PageOrderLine> &pages, PageOrder &order){
  PageOrderLine page;

  for(int i=0; i<(int)order.o_ol_cnt; i++){
    page.page_id = pages.size()+1;
    page.ol_o_id = order.o_id;
    page.ol_d_id = order.o_d_id;
    page.ol_w_id = order.o_w_id;
    page.ol_number = i+1;
    page.ol_i_id = uniform(1, 100000);
    page.ol_supply_w_id = order.o_w_id;
    if(order.o_id < 2101){
      strncpy(page.ol_delivery_d, order.o_entry_d, sizeof(order.o_entry_d));
      page.ol_amount = 0.00;
    } else {
      page.ol_delivery_d[0] = '\0';
      gen_rand_decimal(&page.ol_amount, 1, 999999, 2);
    }
    page.ol_quantity = 5;
    gen_rand_astring(page.ol_dist_info, 24, 24);

    pages.push_back(page);
  }
}

void create_new_order(){
  FD fd;
  std::vector<PageNewOrder> pages(W*10*900);

  for(int i=0; i<W; i++){
    for(int j=0; j<10; j++){
      for(int k=0; k<900; k++){
	int idx = i*10*900 + j*900 + k;

	pages[idx].page_id = idx;
	pages[idx].no_o_id = k+1+2100; // p-key
	pages[idx].no_d_id = j+1; // p-key
	pages[idx].no_w_id = i+1; // p-key
      }
    }
  }

  fd.open(NOFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  fd.write(&pages[0], pages.size()*sizeof(PageNewOrder));

  std::cerr << "create new-order " << pages.size() << "records: " << NOFILENAME << std::endl;
}

static void create_order(){
  FD fd, ol_fd;

  int n = W*10*3000;
  std::vector<PageOrder> pages(n);
  std::vector<PageOrderLine> ol_pages;
  ol_pages.reserve(n*10);

  Permutation p(1,3000);

  for(int i=0; i<W; i++){
    for(int j=0; j<10; j++){
      p.init(1, 3000);
      for(int k=0; k<3000; k++){
	int idx = i*10*3000 + j*3000 + k;

	pages[idx].page_id = idx+1;
	pages[idx].o_id = k+1; // p-key
	pages[idx].o_c_id = p.next();
	pages[idx].o_d_id = j+1; // p-key
	pages[idx].o_w_id = i+1; // p-key
	gen_date_and_time(pages[idx].o_entry_d);
	if(pages[idx].o_id < 2101){
	  pages[idx].o_carrier_id = uniform(1, 10);
	} else {
	  pages[idx].o_carrier_id = 0 ;  // 0 equal to null
	}
	pages[idx].o_ol_cnt = uniform(5, 15);
	pages[idx].o_all_local = 1;

	create_order_line(ol_pages, pages[idx]);
      }
    }
  }

  fd.open(OFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  fd.write(&pages[0], pages.size()*sizeof(PageOrder));

  ol_fd.open(OLFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  ol_fd.write(&ol_pages[0], ol_pages.size()*sizeof(PageOrderLine));

  std::cerr << "create order-line " << ol_pages.size() << "records: " << OFILENAME << std::endl;
  std::cerr << "create new-order " << pages.size() << "records: " << OLFILENAME << std::endl;
}

void create_item(){
  FD fd;
  std::vector<PageItem> pages(100000);

  for(int i=0; i<100000; i++){
    pages[i].page_id = i+1;
    pages[i].i_id = i+1;
    pages[i].i_im_id = uniform(1,10000);
    gen_rand_astring(pages[i].i_name, 14, 24);
    gen_rand_decimal(&pages[i].i_price, 100, 10000, 2);
    gen_rand_astring_with_original(pages[i].i_data, 26, 50, 10);
    gen_rand_astring(pages[i].i_data, 26, 50);
  }

  fd.open(IFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  fd.write(&pages[0], pages.size()*sizeof(PageItem));

  std::cerr << "create item " << pages.size() << "records: " << IFILENAME << std::endl;
}

void create_stock(){
  FD fd;
  std::vector<PageStock> pages(W*100000);

  for(int i=0; i<W; i++){
    for(int j=0; j<100000; j++){
      int idx = i*100000 + j;

      pages[idx].page_id = idx+1;
      pages[idx].s_i_id = j+1;
      pages[idx].s_w_id = i+1;
      pages[idx].s_quantity = uniform(10, 100);
      gen_rand_astring(pages[idx].s_dist_01, 24, 24);
      gen_rand_astring(pages[idx].s_dist_02, 24, 24);
      gen_rand_astring(pages[idx].s_dist_03, 24, 24);
      gen_rand_astring(pages[idx].s_dist_04, 24, 24);
      gen_rand_astring(pages[idx].s_dist_05, 24, 24);
      gen_rand_astring(pages[idx].s_dist_06, 24, 24);
      gen_rand_astring(pages[idx].s_dist_07, 24, 24);
      gen_rand_astring(pages[idx].s_dist_08, 24, 24);
      gen_rand_astring(pages[idx].s_dist_09, 24, 24);
      gen_rand_astring(pages[idx].s_dist_10, 24, 24);
      pages[idx].s_ytd = 0;
      pages[idx].s_order_cnt = 0;
      pages[idx].s_remote_cnt = 0;
      gen_rand_astring_with_original(pages[idx].s_data, 26, 50, 10);
    }
  }

  fd.open(SFILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  fd.write(&pages[0], pages.size()*sizeof(PageStock));

  std::cerr << "create stock " << pages.size()<< "records: " << SFILENAME << std::endl;
}

void create_table(const std::string &tablename){
  if(tablename.compare("warehouse") == 0){
    create_warehouse();
  } else if(tablename.compare("district") == 0){
    create_district();
  } else if(tablename.compare("history") == 0){
    create_history();
  } else if(tablename.compare("new-order") == 0){
    create_new_order();
  } else if(tablename.compare("order") == 0){
    create_order();
  } else if(tablename.compare("item") == 0){
    create_item();
  } else if(tablename.compare("stock") == 0){
    create_stock();
  } else if(tablename.compare("customer") == 0){
    create_customer();
  } else {
    std::cerr << "tablename is invalid!" << std::endl;
  }
}

void create_all(){
  create_warehouse();
  create_district();
  create_history();
  create_new_order();
  create_order();
  //  create_order_line();
  create_item();
  create_stock();
  create_customer();
}

int main(int argc, char *argv[]){
  cmdline::parser p;

  try{
    //    p.add<T>("long option", 'short option', 'must need?', 'default' , CustomReader)
    p.add<int>("scale-factor", 'w', "( 1 - 32 )", false, 1, cmdline::range(1, 32));
    p.add<std::string>("table", 't', "( warehouse | district | history | new-order | order | item | stock | customer )", false, "", cmdline::oneof<std::string>("warehouse", "district", "history", "new-order", "order", "item", "stock", "customer"));
    p.add("help", 0, "print help");

    p.parse_check(argc, argv);

    if(p.exist("scale-factor")){
      W = p.get<int>("scale-factor");
    } else {
      W = 1;
    }


    if(p.exist("table")){
      create_table(p.get<std::string>("table"));
    } else {
      create_all();
    }
  } catch(std::exception e) {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}
