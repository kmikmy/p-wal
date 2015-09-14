#ifndef _workload

#include "tpcc_util.h"
#include <set>

class TpccTransaction{
protected:
  uint32_t xid;
  int thId;
  std::set<std::pair<uint32_t, uint32_t>> lock_table; //<table IDと、page IDからなるロックテーブル>
public:
  TpccTransaction(uint32_t _xid, int _thId);
  virtual int procedure() = 0;
  virtual void unlock_all_page(int thId) = 0;
  void begin();
  void end();
  void run();
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

  bool all_local_item();
public:
  XNewOrder(uint32_t _xid, int _thID, Constant _c);
  int procedure();
  void unlock_all_page(int thId);
};

#endif
