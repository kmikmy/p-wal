#include "tpcc.h"
#include "../ARIES.h"
#include "tpcc_page.h"
#include "tpcc_table.h"
#include "tpcc_util.h"
#include <cstdlib>
#include <iomanip>
#include "../ARIES.h"

#define DEBUG

extern DistributedTransTable *dist_trans_table;

void load_table(TPCC_PAGE *pages, int pagesize, int n, const char *filename);

void
load(){
  load_table(Warehouse::pages, sizeof(PageWarehouse), W, WFILENAME );
  load_table(District::pages, sizeof(PageDistrict), W*10, DFILENAME );
  load_table(Customer::pages, sizeof(PageCustomer), W*10*3000, CFILENAME );
  load_table(Order::pages, sizeof(PageOrder), W*10*3000, OFILENAME );
  load_table(OrderLine::pages, sizeof(PageOrderLine), W*10*3000*15, OLFILENAME );
  load_table(NewOrder::pages, sizeof(PageNewOrder), W*10*900, NOFILENAME );
  load_table(Item::pages, sizeof(PageItem), 100000, IFILENAME );
  load_table(Stock::pages, sizeof(PageStock), W*10*100000, SFILENAME );
}

void
init_table(){
  Warehouse::init();
  District::init();
  Customer::init();
  Order::init();
  OrderLine::init();
  NewOrder::init();
  Item::init();
  Stock::init();
}

void
load_table(TPCC_PAGE *pages, int pagesize, int n, const char *filename){
  int fd;
  if((fd = open(filename, O_RDONLY, 0644)) == -1 ){
    PERR("open");
  }
  int rb = read(fd, pages, pagesize*n);
  if(rb == -1){
    PERR("read");
  }
}

class TpccTransaction{
protected:
  uint32_t xid;
  int thId;
public:
  TpccTransaction(uint32_t _xid, int _thId){
    xid = _xid;
    thId = _thId;
  }
  
  //  virtual void run() = 0;
  virtual void procedure() = 0;

  void
  begin(){
    Log log;
    memset(&log,0,sizeof(log));
    log.TransID = xid;
    log.Type = BEGIN;

    Logger::log_write(&log,thId); // log.LSNが代入される

    dist_trans_table[thId].TransID = xid;
    dist_trans_table[thId].LastLSN = log.LSN;
    dist_trans_table[thId].LastOffset = log.Offset;
    dist_trans_table[thId].UndoNxtLSN = log.LSN; /* BEGIN レコードは END レコードによってコンペンセーションされる. */
    dist_trans_table[thId].LastOffset = log.Offset;
  

#ifdef DEBUG
    Logger::log_debug(log);  
#endif
  }

  void 
  end(){
    Log log;
    memset(&log,0,sizeof(log));
    log.TransID = xid;
    log.Type = END;
    log.PrevLSN = dist_trans_table[thId].LastLSN;
    log.PrevOffset = dist_trans_table[thId].LastOffset;
    log.UndoNxtLSN = 0; /* 一度 END ログが書かれたトランザクションは undo されることはない */
    log.UndoNxtOffset = 0; 

    Logger::log_write(&log,thId);

    /* dist_transaction_tableのエントリを削除する. */
    memset(&dist_trans_table[thId], 0, sizeof(DistributedTransTable));

#ifdef DEBUG
    Logger::log_debug(log);  
#endif
  }

  void 
  run(){
    begin();
    procedure();
    end();
  }
};

class XNewOrder : public TpccTransaction {
private:
  uint32_t w_id;
  Constant c;

  /* input data */  
  uint32_t d_id;
  uint32_t c_id;
  uint32_t rbk;

  uint32_t ol_cnt;  
  uint32_t* ol_i_id;
  uint32_t* ol_supply_w_id;
  uint32_t* ol_quantity;

  char o_entry_d[26];

  bool
  all_local_item(){
    bool ret = true;
    for(uint32_t i=0; i<ol_cnt; i++){
      if(ol_supply_w_id[i] != w_id){
	ret = false;
	break;
      }
    }
    return ret;
  }

  void
  rollback(){

  }

public:
  XNewOrder(uint32_t xid, int thID, Constant _c) : TpccTransaction(xid, thId){
    w_id = uniform(1,W);
    c = _c;
  }

  void procedure(){
    d_id = uniform(1, 10); /* district id  */
    c_id = NURand(1023,1,3000,c.c_for_c_id); /* customer id */
    ol_cnt = uniform(5, 15); /* the number of items */
    rbk = uniform(1, 100); /* rbj is used to choose rollback transaction */

    try {
      ol_i_id = new uint32_t[ol_cnt]; 
      ol_supply_w_id = new uint32_t[ol_cnt];
      ol_quantity = new uint32_t[ol_cnt];
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }

    for(uint32_t i=0;i<ol_cnt;i++){
      ol_i_id[i] = NURand(8191,1,100000,c.c_for_ol_i_id); /* choose item number */
      if(rbk == 1 && i == ol_cnt-1){ /* A fixed 1% of the New-Order transactions do rollback */
	ol_i_id[i] = 999999;
      }
      
      if( 1 < uniform(1,100)){
	ol_supply_w_id[i] = w_id;
      } else {
	do{
	  ol_supply_w_id[i] = uniform(1,W);
	}while( W != 1 && ol_supply_w_id[i] == w_id );
      }
      ol_quantity[i] = uniform(1,10);
    }
    gen_date_and_time(o_entry_d);
    
    PageWarehouse *wp = Warehouse::select1(w_id);
    double w_tax = wp->w_tax;

    PageDistrict *dp = District::select1(w_id, d_id);
    double d_tax = dp->d_tax;
    uint32_t o_id;
    uint32_t d_next_o_id = dp->d_next_o_id;
    o_id = dp->d_next_o_id;
    ++d_next_o_id;
    District::update1(d_next_o_id, dp);

    PageCustomer *cp = Customer::select1(w_id, d_id, c_id);
    double c_discount = cp->c_discount;
    char *c_last = cp->c_last;
    char *c_credit = cp->c_credit;

    
    PageNewOrder nop;
    {
      nop.no_o_id = o_id;
      nop.no_d_id = d_id;
      nop.no_w_id = w_id;
    }
    PageOrder op;
    {
      op.o_id = o_id;
      op.o_d_id = d_id;
      op.o_w_id = w_id;
      op.o_c_id = c_id;
      strncpy(op.o_entry_d, o_entry_d,strlen(o_entry_d)+1);
      op.o_carrier_id = 0;
      op.o_ol_cnt = ol_cnt;
      if(all_local_item())
	op.o_all_local = 1;
    }
    NewOrder::insert1(nop);
    Order::insert1(op);

    PageItem **ip;
    double *i_price;
    char (*i_name)[25];
    char (*i_data)[51];

    PageStock **sp;
    uint32_t *s_quantity;
    uint32_t *s_ytd;
    uint32_t *s_order_cnt;
    uint32_t *s_remote_cnt;
    char (*s_dist_xx)[25];
    char (*s_data)[51];

    PageOrderLine *olp;
    char (*brand_generic)[2];
    
    try {
      ip = new PageItem*[ol_cnt];
      i_price = new double[ol_cnt];
      i_name = new char[ol_cnt][25];
      i_data = new char[ol_cnt][51];

      sp = new PageStock*[ol_cnt];
      s_quantity = new uint32_t[ol_cnt];
      s_ytd = new uint32_t[ol_cnt];
      s_order_cnt = new uint32_t[ol_cnt];;
      s_remote_cnt = new uint32_t[ol_cnt];;
      s_dist_xx = new char[ol_cnt][25];
      s_data = new char[ol_cnt][51];

      olp = new PageOrderLine[ol_cnt];
      brand_generic = new char[ol_cnt][2];
    }
    catch(std::bad_alloc e) {
      PERR("new");
    }

    double total_amount = 0.0;

    for(uint32_t i=0;i<ol_cnt;i++){    
      ip[i] = Item::select1(ol_i_id[i]);
      if(ip[i] == NULL){
	rollback();
      }

      i_price[i] = ip[i]->i_price;
      strncpy(i_name[i], ip[i]->i_name, strlen(ip[i]->i_name)+1);
      strncpy(i_data[i], ip[i]->i_data, strlen(ip[i]->i_data)+1);
      
      sp[i] = Stock::select1(ol_i_id[i], ol_supply_w_id[i]);
      s_quantity[i] = sp[i]->s_quantity;
      s_ytd[i] = sp[i]->s_ytd;
      s_order_cnt[i] = sp[i]->s_order_cnt;
      s_remote_cnt[i] = sp[i]->s_remote_cnt;
      switch(d_id){
      case 1: strncpy(s_dist_xx[i], sp[i]->s_dist_01, strlen(sp[i]->s_dist_01)+1); 
      case 2: strncpy(s_dist_xx[i], sp[i]->s_dist_02, strlen(sp[i]->s_dist_02)+1); 
      case 3: strncpy(s_dist_xx[i], sp[i]->s_dist_03, strlen(sp[i]->s_dist_03)+1); 
      case 4: strncpy(s_dist_xx[i], sp[i]->s_dist_04, strlen(sp[i]->s_dist_04)+1); 
      case 5: strncpy(s_dist_xx[i], sp[i]->s_dist_05, strlen(sp[i]->s_dist_05)+1); 
      case 6: strncpy(s_dist_xx[i], sp[i]->s_dist_06, strlen(sp[i]->s_dist_06)+1); 
      case 7: strncpy(s_dist_xx[i], sp[i]->s_dist_07, strlen(sp[i]->s_dist_07)+1);  
      case 8: strncpy(s_dist_xx[i], sp[i]->s_dist_08, strlen(sp[i]->s_dist_08)+1); 
      case 9: strncpy(s_dist_xx[i], sp[i]->s_dist_09, strlen(sp[i]->s_dist_09)+1); 
      case 10: strncpy(s_dist_xx[i], sp[i]->s_dist_10, strlen(sp[i]->s_dist_10)+1); 
      }
      strncpy(s_data[i], sp[i]->s_data, strlen(sp[i]->s_data)+1); 
      
      if(s_quantity[i] >= ol_quantity[i]+10){
	s_quantity[i] = s_quantity[i] - ol_quantity[i];
      } else {
	s_quantity[i] = s_quantity[i] - ol_quantity[i] + 91;
      }
      ++s_ytd[i];
      ++s_order_cnt[i];
      if(ol_supply_w_id[i] != w_id )
	++s_remote_cnt[i];
      
      Stock::update1(s_quantity[i], s_ytd[i], s_order_cnt[i], s_remote_cnt[i], sp[i]);

      olp[i].ol_amount = ol_quantity[i] * i_price[i];
      if(strstr(ip[i]->i_data, "ORIGINAL") && strstr(sp[i]->s_data, "ORIGINAL")){
	strncpy(brand_generic[i],"B",2);
      } else {
	strncpy(brand_generic[i],"G",2);
      }
      strncpy(olp[i].ol_delivery_d, "", 2);
      olp[i].ol_o_id = o_id;
      olp[i].ol_number = i;
      strncpy(olp[i].ol_dist_info, s_dist_xx[i], strlen(s_dist_xx[i])+1);
      OrderLine::insert1(olp[i]);
      total_amount += olp[i].ol_amount;
    }

    total_amount *= (1-c_discount) * (1+w_tax+d_tax);
    
#ifdef DEBUG
    std::cout << "*** New Order ***" << std::endl;
    std::cout << "Warehouse:\t" << w_id << "\tDistrict:\t" << d_id << std::endl;
    std::cout << "Customer:\t" << c_id << "\tName:\t" << c_last << "\tCredit:\t" << c_credit << std::endl;
    std::cout << "Customer Discount:\t" << c_discount << "\tWarehouse Tax:\t" << w_tax << "\tDistrict Tax:\t" << d_tax << std::endl;
    std::cout << "Order Number:\t" << o_id << "\tNumber of Lines:\t" << ol_cnt << std::endl;
    std::cout << "Order Entry Date:\t" << o_entry_d << "\tTotal Amount:\t" << total_amount << std::endl << std::endl;

    
    std::cout << "Supp_W\t" << "Item_ID\t" << std::left << std::setw(25)<< "Item Name\t" << "Qty\t" << "S\t" << "BG\t" << "Price\t" << "Amount" << std::endl;
    for(uint32_t i=0;i<ol_cnt;i++){
      std::cout << ol_supply_w_id[i] << "\t" << ol_i_id[i] << "\t" << std::setw(25) << i_name[i] << "\t" << ol_quantity[i] << "\t" << s_quantity[i] << "\t" << brand_generic[i] << "\t" << i_price[i] << "\t" << olp[i].ol_amount << std::endl;
    }
    std::cout << std::endl;
#endif    
    
  }
};


Constant c;

int main(){
  //  Constant C;
  // std::cout << c.c_for_c_last << "," << c.c_for_c_id << "," << c.c_for_ol_i_id << std::endl;
  init_table();
  load();
  XNewOrder x(0, 0, c);
  x.run();
  

  return 0;
}
