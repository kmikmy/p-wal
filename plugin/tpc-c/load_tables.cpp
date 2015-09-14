#include <getopt.h>
#include <unistd.h>
#include <cstdio>
#include <map>
#include <string>
#include "include/tpcc.h"
#include "include/tpcc_page.h"
#include "include/debug.h"


typedef std::map<std::string, void(*)()> funcs_type;

funcs_type funcs;

void load_warehouse(){
  int fd;
  if((fd = open(WFILENAME, O_RDONLY, 0644)) == -1 ){
    PERR("open");
  }

  PageWarehouse p[BUFSIZ];
  while(1){
    int rb = read(fd, p, sizeof(PageWarehouse)*BUFSIZ);
    if(rb == -1){
      PERR("read");
    } else if(rb == 0){
      break;
    }

    int nrecord = rb/sizeof(PageWarehouse);

      std::cout << "[PID]: "
		<< "w_id, "
		<< "w_name, "
		<< "w_street_1, "
		<< "w_street_2, "
		<< "w_city, "
		<< "w_state, "
		<< "w_zip, "
		<< "w_tax, "
		<< "w_ytd" 
		<< std::endl;

    for(int i=0;i<nrecord;i++){
      std::cout << '['
		<< p[i].page_id << ']'
		<< ':' << p[i].w_id
		<< ", " << p[i].w_name
		<< ", " << p[i].w_street_1
		<< ", " << p[i].w_street_2
		<< ", " << p[i].w_city
		<< ", " << p[i].w_state
		<< ", " << p[i].w_zip
		<< ", " << p[i].w_tax
		<< ", " << p[i].w_ytd
		<< std::endl;
    }
    std::cout << std::endl;
  }
}

void load_district(){
  int fd;
  if((fd = open(DFILENAME, O_RDONLY, 0644)) == -1 ){
    PERR("open");
  }

  PageDistrict p[BUFSIZ];
  while(1){
    int rb = read(fd, p, sizeof(PageDistrict)*BUFSIZ);
    if(rb == -1){
      PERR("read");
    } else if(rb == 0){
      break;
    }

    int nrecord = rb/sizeof(PageDistrict);

      std::cout << "[PID]: "
		<< "d_id, "
		<< "d_w_id, "
		<< "d_name, "
		<< "d_street_1, "
		<< "d_street_2, "
		<< "d_city, "
		<< "d_state, "
		<< "d_zip, "
		<< "d_tax, "
		<< "d_ytd, " 
		<< "d_next_o_id"
		<< std::endl;

    for(int i=0;i<nrecord;i++){
      std::cout << '['
		<< p[i].page_id << ']'
		<< ':' << p[i].d_id
		<< ", " << p[i].d_w_id
		<< ", " << p[i].d_name
		<< ", " << p[i].d_street_1
		<< ", " << p[i].d_street_2
		<< ", " << p[i].d_city
		<< ", " << p[i].d_state
		<< ", " << p[i].d_zip
		<< ", " << p[i].d_tax
		<< ", " << p[i].d_ytd
		<< ", " << p[i].d_next_o_id
		<< std::endl;
    }
    std::cout << std::endl;
  }
}

void load_history(){
  int fd;
  if((fd = open(HFILENAME, O_RDONLY, 0644)) == -1 ){
    PERR("open");
  }

  PageHistory p[BUFSIZ];
  while(1){
    int rb = read(fd, p, sizeof(PageHistory)*BUFSIZ);
    if(rb == -1){
      PERR("read");
    } else if(rb == 0){
      break;
    }

    int nrecord = rb/sizeof(PageHistory);

      std::cout << "[PID]: "
		<< "h_c_id, "
		<< "h_c_d_id, "
		<< "h_d_id, "
		<< "h_w_id, "
		<< "h_date, "
		<< "h_amount, "
		<< "h_date"
		<< std::endl;

    for(int i=0;i<nrecord;i++){
      std::cout << '['
		<< p[i].page_id << ']'
		<< ':' << p[i].h_c_id
		<< ", " << p[i].h_c_d_id
		<< ", " << p[i].h_d_id
		<< ", " << p[i].h_w_id
		<< ", " << p[i].h_date
		<< ", " << p[i].h_amount
		<< ", " << p[i].h_data
		<< std::endl;
    }
    std::cout << std::endl;
  }
}

void load_new_order(){
  int fd;
  if((fd = open(NOFILENAME, O_RDONLY, 0644)) == -1 ){
    PERR("open");
  }

  PageNewOrder p[BUFSIZ];
  while(1){
    int rb = read(fd, p, sizeof(PageNewOrder)*BUFSIZ);
    if(rb == -1){
      PERR("read");
    } else if(rb == 0){
      break;
    }

    int nrecord = rb/sizeof(PageNewOrder);

      std::cout << "[PID]: "
		<< "no_o_id, "
		<< "no_d_id, "
		<< "no_w_id, "
		<< std::endl;

    for(int i=0;i<nrecord;i++){
      std::cout << '['
		<< p[i].page_id << ']'
		<< ':' << p[i].no_o_id
		<< ", " << p[i].no_d_id
		<< ", " << p[i].no_w_id
		<< std::endl;
    }
    std::cout << std::endl;
  }
}

void load_order(){
  int fd;
  if((fd = open(OFILENAME, O_RDONLY, 0644)) == -1 ){
    PERR("open");
  }

  PageOrder p[BUFSIZ];
  while(1){
    int rb = read(fd, p, sizeof(PageOrder)*BUFSIZ);
    if(rb == -1){
      PERR("read");
    } else if(rb == 0){
      break;
    }

    int nrecord = rb/sizeof(PageOrder);

      std::cout << "[PID]: "
		<< "o_id, "
		<< "o_d_id, "
		<< "o_w_id, "
		<< "o_c_id, "
		<< "o_entry_d, "
		<< "o_carrier_id, "
		<< "o_ol_cnt, "
		<< "o_all_local, "
		<< std::endl;

    for(int i=0;i<nrecord;i++){
      std::cout << '['
		<< p[i].page_id << ']'
		<< ':' << p[i].o_id
		<< ',' << p[i].o_d_id
		<< ',' << p[i].o_w_id
		<< ',' << p[i].o_c_id
		<< ',' << p[i].o_entry_d
		<< ',' << p[i].o_ol_cnt
		<< ',' << p[i].o_all_local
		<< std::endl;
    }
    std::cout << std::endl;
  }
}

void load_order_line(){
  int fd;
  if((fd = open(OLFILENAME, O_RDONLY, 0644)) == -1 ){
    PERR("open");
  }

  PageOrderLine p[BUFSIZ];
  while(1){
    int rb = read(fd, p, sizeof(PageOrderLine)*BUFSIZ);
    if(rb == -1){
      PERR("read");
    } else if(rb == 0){
      break;
    }

    int nrecord = rb/sizeof(PageOrderLine);

      std::cout << "[PID]: "
		<< "ol_o_id, "
		<< "ol_d_id, "
		<< "ol_w_id, "
		<< "ol_number, "
		<< "ol_i_id, "
		<< "ol_supply_w_id, "
		<< "ol_delivery_d, "
		<< "ol_quantity, "
		<< "ol_amount, "
		<< "ol_dist_info, "
		<< std::endl;

    for(int i=0;i<nrecord;i++){
      std::cout << '['
		<< p[i].page_id << ']'
		<< ':' << p[i].ol_o_id
		<< ',' << p[i].ol_d_id
		<< ',' << p[i].ol_w_id
		<< ',' << p[i].ol_number
		<< ',' << p[i].ol_i_id
		<< ',' << p[i].ol_supply_w_id
		<< ',' << p[i].ol_delivery_d
		<< ',' << p[i].ol_quantity
		<< ',' << p[i].ol_amount
		<< ',' << p[i].ol_dist_info
		<< std::endl;
    }
    std::cout << std::endl;
  }
}

void load_item(){
  int fd;
  if((fd = open(IFILENAME, O_RDONLY, 0644)) == -1 ){
    PERR("open");
  }

  PageItem p[BUFSIZ];
  while(1){
    int rb = read(fd, p, sizeof(PageItem)*BUFSIZ);
    if(rb == -1){
      PERR("read");
    } else if(rb == 0){
      break;
    }

    int nrecord = rb/sizeof(PageItem);

      std::cout << "[PID]: "
		<< "i_id, "
		<< "i_im_id, "
		<< "i_name, "
		<< "i_price, "
		<< "i_data, "
		<< std::endl;

    for(int i=0;i<nrecord;i++){
      std::cout << '['
		<< p[i].page_id << ']'
		<< ':' << p[i].i_id
		<< ',' << p[i].i_im_id
		<< ',' << p[i].i_name
		<< ',' << p[i].i_price
		<< ',' << p[i].i_data
		<< std::endl;
    }
    std::cout << std::endl;
  }
}

void load_stock(){
  int fd;
  if((fd = open(SFILENAME, O_RDONLY, 0644)) == -1 ){
    PERR("open");
  }

  PageStock p[BUFSIZ];
  while(1){
    int rb = read(fd, p, sizeof(PageStock)*BUFSIZ);
    if(rb == -1){
      PERR("read");
    } else if(rb == 0){
      break;
    }

    int nrecord = rb/sizeof(PageStock);

      std::cout << "[PID]: "
		<< "s_i_id, "
		<< "s_w_id, "
		<< "s_quantity, "
		<< "s_dist_01, "
		<< "s_dist_02, "
		<< "s_dist_03, "
		<< "s_dist_04, "
		<< "s_dist_05, "
		<< "s_dist_06, "
		<< "s_dist_07, "
		<< "s_dist_08, "
		<< "s_dist_09, "
		<< "s_dist_10, "
		<< "s_ytd, "
		<< "s_order_cnt, "
		<< "s_remote_cnt, "
		<< "s_data, "
		<< std::endl;

    for(int i=0;i<nrecord;i++){
      std::cout << '['
		<< p[i].page_id << ']'
		<< ':' << p[i].s_i_id
		<< ',' << p[i].s_w_id
		<< ',' << p[i].s_quantity
		<< ',' << p[i].s_dist_01
		<< ',' << p[i].s_dist_02
		<< ',' << p[i].s_dist_03
		<< ',' << p[i].s_dist_04
		<< ',' << p[i].s_dist_05
		<< ',' << p[i].s_dist_06
		<< ',' << p[i].s_dist_07
		<< ',' << p[i].s_dist_08
		<< ',' << p[i].s_dist_09
		<< ',' << p[i].s_dist_10
		<< ',' << p[i].s_ytd
		<< ',' << p[i].s_order_cnt
		<< ',' << p[i].s_remote_cnt
		<< ',' << p[i].s_data
		<< std::endl;
    }
    std::cout << std::endl;
  }
}

void load_customer(){
  int fd;
  if((fd = open(CFILENAME, O_RDONLY, 0644)) == -1 ){
    PERR("open");
  }

  PageCustomer p[BUFSIZ];
  while(1){
    int rb = read(fd, p, sizeof(PageCustomer)*BUFSIZ);
    if(rb == -1){
      PERR("read");
    } else if(rb == 0){
      break;
    }

    int nrecord = rb/sizeof(PageCustomer);

      std::cout << "[PID]: "
		<< "c_id, "
		<< "c_d_id, "
		<< "c_w_id, "
		<< "c_first, "
		<< "c_middle, "
		<< "c_last, "
		<< "c_street_1, "
		<< "c_street_2, "
		<< "c_city, "
		<< "c_state, "
		<< "c_zip, "
		<< "c_phone, "
		<< "c_since, "
		<< "c_credit, "
		<< "c_credit_lim, "
		<< "c_discount, "
		<< "c_balance, "
		<< "c_credit, "
		<< "c_ytd_payment, "
		<< "c_payment_cnt, "
		<< "c_delivery_cnt, "
		<< "c_data, "
		<< std::endl;

    for(int i=0;i<nrecord;i++){
      std::cout << '['
		<< p[i].page_id << ']'
		<< ':' << p[i].c_id
		<< ',' << p[i].c_d_id
		<< ',' << p[i].c_w_id
		<< ',' << p[i].c_first
		<< ',' << p[i].c_middle
		<< ',' << p[i].c_last
		<< ',' << p[i].c_street_1
		<< ',' << p[i].c_street_2
		<< ',' << p[i].c_city
		<< ',' << p[i].c_state
		<< ',' << p[i].c_zip
		<< ',' << p[i].c_phone
		<< ',' << p[i].c_since
		<< ',' << p[i].c_credit
		<< ',' << p[i].c_credit_lim
		<< ',' << p[i].c_discount
		<< ',' << p[i].c_balance
		<< ',' << p[i].c_credit
		<< ',' << p[i].c_ytd_payment
		<< ',' << p[i].c_payment_cnt
		<< ',' << p[i].c_delivery_cnt
		<< ',' << p[i].c_data
		<< std::endl;
    }
    std::cout << std::endl;
  }
}

std::pair<std::string, void(*)()>
make_pair(std::string s, void(*ptr)()){
  return std::pair<std::string, void(*)()>(s, ptr);
}

void
execute_cmd(std::string str){
  funcs_type::iterator it = funcs.find(str);
  if (it != funcs.end()){
    it->second();
  } else {
    std::cout << "not found" << std::endl;
  }
}

void
all(){
  funcs_type::iterator it;
  for(it=funcs.begin(); it!=funcs.end(); it++){
    it->second();
  }
}

void
register_func(){
  funcs.insert(make_pair("warehouse", load_warehouse));
  funcs.insert(make_pair("district", load_district));
  funcs.insert(make_pair("history", load_history));
  funcs.insert(make_pair("new_order", load_new_order));
  funcs.insert(make_pair("order", load_order));
  funcs.insert(make_pair("order_line", load_order_line));
  funcs.insert(make_pair("item", load_item));
  funcs.insert(make_pair("stock", load_stock));
  funcs.insert(make_pair("customer", load_customer));
}

int
main(int argc, char** argv) {
  int result = 0;
  std::string str;

  register_func();

  while ((result = getopt(argc, argv, "t:")) != -1) {
    switch(result){
    case 't':
      str = optarg;
      printf("option %c applied with %s\n", result, str.c_str());
      execute_cmd(str);
      break;
    case ':':   // no value applied
      all();
      break;
    case '?':   // invalid option
      exit(1);
    }
  }
}
