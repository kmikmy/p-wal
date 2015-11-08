#ifndef _tpcc_page
#define _tpcc_page

#include <string>

const std::string WFILENAME = "/home/kamiya/hpcs/aries/plugin/tpc-c/table/warehouse.dat";
const std::string DFILENAME = "/home/kamiya/hpcs/aries/plugin/tpc-c/table/district.dat";
const std::string CFILENAME = "/home/kamiya/hpcs/aries/plugin/tpc-c/table/customer.dat";
const std::string HFILENAME = "/home/kamiya/hpcs/aries/plugin/tpc-c/table/history.dat";
const std::string NOFILENAME = "/home/kamiya/hpcs/aries/plugin/tpc-c/table/new_order.dat";
const std::string OFILENAME = "/home/kamiya/hpcs/aries/plugin/tpc-c/table/order.dat";
const std::string OLFILENAME = "/home/kamiya/hpcs/aries/plugin/tpc-c/table/order_line.dat";
const std::string SFILENAME = "/home/kamiya/hpcs/aries/plugin/tpc-c/table/stock.dat";
const std::string IFILENAME = "/home/kamiya/hpcs/aries/plugin/tpc-c/table/item.dat";

/* CLOADFILENAME = is filename for C-Load*/
const std::string CLOADFILENAME = "/home/kamiya/hpcs/aries/plugin/tpc-c/table/c_load_c_last.dat";

class TPCC_PAGE{
 public:
  uint64_t page_LSN;
  uint32_t page_id;
  bool delete_flag;
};

class TPCC_PAGE_LIST{
 public:
  TPCC_PAGE* page;
  TPCC_PAGE_LIST* bfr;
  TPCC_PAGE_LIST* nxt;
};


/*
  Warehouse

  PK: w_id
*/
class PageWarehouse : public TPCC_PAGE{
 public:
  uint32_t w_id; // 2*W のユニークなIDの範囲(W個のWarehouseがある)
  char w_name[11]; // varchar(10)
  char w_street_1[21]; // varchar(20)
  char w_street_2[21]; // varchar(20)
  char w_city[21]; // varchar(20)
  char w_state[3]; // char(2)
  char w_zip[10]; // char(9)
  double w_tax; // DECIMAL(4,4) 全部で四桁(少数点以下4桁)
  double w_ytd; // DECIMAL(12,2) year to date balance 今年の総売上
};


/*
  District

  PK: D_W_ID, D_ID
  FK: D_W_ID(ref:W_ID)
*/
class PageDistrict: public TPCC_PAGE{
 public:
  uint32_t d_id; // 20 のユニークなIDの範囲(Warehouse毎に10個ある)
  uint32_t d_w_id; // FK, references W_ID
  char d_name[21]; // varchar(20)
  char d_street_1[21]; // varchar(20)
  char d_street_2[21]; // varchar(20)
  char d_city[21]; // varchar(20)
  char d_state[3]; // char(2)
  char d_zip[10]; // char(9)
  double d_tax; // DECIMAL(4,4) 全部で四桁(少数点以下4桁)
  double d_ytd; // DECIMAL(12,2)
  uint32_t d_next_o_id; // 10,000,000のユニークな値の範囲．次の利用可能な注文番号
};

/*
  Customer

  PK: C_W_ID, C_D_ID, C_ID
  FK: C_W_ID(ref:D_W_ID), C_D_ID(D_ID)
*/
class PageCustomer: public TPCC_PAGE{
 public:
  uint32_t c_id; // 96,000のユニークなID．地区ごとに3,000の消費者がいる
  uint32_t c_d_id; // 20のユニークID
  uint32_t c_w_id; // 2*WのユニークID
  char c_first[17]; // varchar(16)
  char c_middle[17]; // varchar(16)
  char c_last[17]; // varchar(16)
  char c_street_1[21]; // varchar(20)
  char c_street_2[21]; // varchar(20)
  char c_city[21]; // varchar(20)
  char c_state[3]; // char(2)
  char c_zip[10]; // char(9)
  char c_phone[17]; //char(16)
  char c_since[32]; // datetime # "Fri May 11 21:44:53 2001\n\0" may be 26
  char c_credit[3]; // char(2) "GC=good", "BC"=bad
  double c_credit_lim; // DECIMAL(12,2)
  double c_discount; // DECIMAL(4,4)
  double c_balance; // DECIMAL(12,2)
  double c_ytd_payment; // DECIMAL(12,2)
  uint32_t c_payment_cnt; // DECIMAL(4,0)
  uint32_t c_delivery_cnt; // DECIMAL(4,0)
  char c_data[501]; // varchar(500)
};

/*
  History

  PK: none
  FK: h_c_w_id(ref: c_w_id), h_c_d_id(ref: c_d_id), h_c_id(ref: c_id)

  Comment: ，ベンチマークのコンテキスト内で，このテーブル内の行を一意に特定する必要がないので，ヒストリーテーブルの行はpkを持たない．
  Note: TPC-Cアプリケーションは6,000を超えたC_IDの範囲を利用できる必要はない．

*/
class PageHistory: public TPCC_PAGE{
 public:
  uint32_t h_c_id; // 96,000のユニークID
  uint32_t h_c_d_id; // 20のユニークID
  uint32_t h_c_w_id; // 2*WのユニークID
  uint32_t h_d_id; // 20のユニークID
  uint32_t h_w_id; // 2*WのユニークID
  char h_date[32]; // datetime # "Fri May 11 21:44:53 2001\n\0" may be 26
  double h_amount; // DECIMAL(6,2)
  char h_data[25]; // varchaR(24)
};

/*
  NewOrder

  PK: no_w_id, no_d_id, no_o_id
  FK: no_w_id(ref: o_w_id), no_d_id(ref: o_d_id), no_o_id(ref: o_id)
*/
class PageNewOrder: public TPCC_PAGE{
 public:
  uint32_t no_o_id; // 10,000,000のユニークID
  uint32_t no_d_id; // 20のユニークID
  uint32_t no_w_id; // 2*WのユニークID
};

/*
  Order

  PK: o_w_id, o_d_id, o_id
  FK: o_w_id(ref: c_w_id), o_d_id(ref: c_d_id), o_c_id(ref: c_id)
 */
class PageOrder: public TPCC_PAGE{
 public:
  uint32_t o_id; // 10,000,000のユニークID
  uint32_t o_d_id; // 20のユニークID
  uint32_t o_w_id; // 2*WのユニークID
  uint32_t o_c_id; // 2*WのユニークID
  char o_entry_d[32]; // datetime # "Fri May 11 21:44:53 2001\n\0" may be 26
  uint32_t o_carrier_id; // 10のユニークID, or null
  uint32_t o_ol_cnt; // DECIMAL(2,0)．Order-Linesの数
  uint32_t o_all_local; // DECIMAL(1,0)
};


/*
  OrderLine

  PK: ol_w_id, ol_d_id, ol_o_id, ol_number
  FK: ol_w_id(ref: o_w_id), ol_d_id(ref: o_d_id), ol_o_id(ref: o_id)
      ol_supply_w_id(ref: s_w_id), ol_i_id(ref: s_i_id)
*/
class PageOrderLine: public TPCC_PAGE{
 public:
  uint32_t ol_o_id; // 10,000,000のユニークID
  uint32_t ol_d_id; // 20のユニークID
  uint32_t ol_w_id; // 2*WのユニークID
  uint32_t ol_number; // 15のユニークID
  uint32_t ol_i_id; // 200,000のユニークID
  uint32_t ol_supply_w_id; // 2*WのユニークID
  char ol_delivery_d[20]; // date and time, or null numeric(2)
  uint32_t ol_quantity; // numeric(2)
  double ol_amount;  // signed numeric(6, 2)
  char ol_dist_info[25]; // fixed text, size 24
};

/*
  ITEM

  PK: I_ID
 */
class PageItem: public TPCC_PAGE{
 public:
  uint32_t i_id; // 200,000のユニークID．100,000アイテムが存在する
  uint32_t i_im_id; // 200,000のユニークID． Itemに関連する画像ID
  char i_name[25]; // varchar(24)
  double i_price; // numeric(5,2)
  char i_data[51]; // varchar(50)．Brand infromation
};

/*
  STOCK

  PK: s_w_id, s_i_id
  FK: s_w_id(ref: w_id), s_i_id(ref: i_id)
 */
class PageStock: public TPCC_PAGE{
 public:
  uint32_t s_i_id; // 200,000のユニークID．100,000アイテムが存在する
  uint32_t s_w_id; // 2*WのユニークID
  uint32_t s_quantity; // signed numberic(4)
  char s_dist_01[25]; // fixed tex, size 24
  char s_dist_02[25]; // fixed tex, size 24
  char s_dist_03[25]; // fixed tex, size 24
  char s_dist_04[25]; // fixed tex, size 24
  char s_dist_05[25]; // fixed tex, size 24
  char s_dist_06[25]; // fixed tex, size 24
  char s_dist_07[25]; // fixed tex, size 24
  char s_dist_08[25]; // fixed tex, size 24
  char s_dist_09[25]; // fixed tex, size 24
  char s_dist_10[25]; // fixed tex, size 24
  uint32_t s_ytd; // numeric(8)
  uint32_t s_order_cnt; // numeric(8)
  uint32_t s_remote_cnt; // numeric(4)
  char s_data[51]; // varchar(50)．Make information
};

#endif
