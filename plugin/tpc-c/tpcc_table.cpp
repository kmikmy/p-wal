#include "include/tpcc.h"
#include "include/tpcc_table.h"
#include "../../include/ARIES.h"
#include <cstdlib>

extern uint64_t update(const char* table_name, const std::vector<QueryArg> &qs, uint32_t page_id, uint32_t xid, uint32_t thId);
extern uint64_t insert(const char* table_name, const std::vector<QueryArg> &qs, uint32_t page_id, uint32_t xid, uint32_t thId);


PageWarehouse *Warehouse::pages;
PageDistrict *District::pages;
PageCustomer *Customer::pages;
PageNewOrder *NewOrder::pages;
PageOrder *Order::pages;
PageItem *Item::pages;
PageStock *Stock::pages;
PageOrderLine *OrderLine::pages;


uint32_t Warehouse::npage;
uint32_t Warehouse::loaded_npage;
uint32_t Warehouse::inserted_npage;
pthread_mutex_t Warehouse::insert_lock;
TPCC_PAGE_LIST *Warehouse::first_appended_page;
TPCC_PAGE_LIST *Warehouse::last_appended_page;
pthread_rwlock_t *Warehouse::locks;
std::set<uint32_t> Warehouse::my_lock_table[32];
uint32_t District::npage;
uint32_t District::loaded_npage;
uint32_t District::inserted_npage;
pthread_mutex_t District::insert_lock;
TPCC_PAGE_LIST *District::first_appended_page;
TPCC_PAGE_LIST *District::last_appended_page;
pthread_rwlock_t *District::locks;
std::set<uint32_t> District::my_lock_table[32];

uint32_t Customer::npage;
uint32_t Customer::loaded_npage;
uint32_t Customer::inserted_npage;
pthread_mutex_t Customer::insert_lock;
TPCC_PAGE_LIST *Customer::first_appended_page;
TPCC_PAGE_LIST *Customer::last_appended_page;
pthread_rwlock_t *Customer::locks;
std::set<uint32_t> Customer::my_lock_table[32];

uint32_t NewOrder::npage;
uint32_t NewOrder::loaded_npage;
uint32_t NewOrder::inserted_npage;
pthread_mutex_t NewOrder::insert_lock;
TPCC_PAGE_LIST *NewOrder::first_appended_page[MAX_WORKER_THREAD];
TPCC_PAGE_LIST *NewOrder::last_appended_page[MAX_WORKER_THREAD];
pthread_rwlock_t *NewOrder::locks;
std::set<uint32_t> NewOrder::my_lock_table[32];

uint32_t Order::npage;
uint32_t Order::loaded_npage;
uint32_t Order::inserted_npage;
pthread_mutex_t Order::insert_lock;
TPCC_PAGE_LIST *Order::first_appended_page[MAX_WORKER_THREAD];
TPCC_PAGE_LIST *Order::last_appended_page[MAX_WORKER_THREAD];
pthread_rwlock_t *Order::locks;
std::set<uint32_t> Order::my_lock_table[32];

uint32_t Item::npage;
uint32_t Item::loaded_npage;
uint32_t Item::inserted_npage;
pthread_mutex_t Item::insert_lock;
TPCC_PAGE_LIST *Item::first_appended_page;
TPCC_PAGE_LIST *Item::last_appended_page;
pthread_rwlock_t *Item::locks;
std::set<uint32_t> Item::my_lock_table[32];

uint32_t Stock::npage;
uint32_t Stock::loaded_npage;
uint32_t Stock::inserted_npage;
pthread_mutex_t Stock::insert_lock;
TPCC_PAGE_LIST *Stock::first_appended_page;
TPCC_PAGE_LIST *Stock::last_appended_page;
pthread_rwlock_t *Stock::locks;
std::set<uint32_t> Stock::my_lock_table[32];

uint32_t OrderLine::npage;
uint32_t OrderLine::loaded_npage;
uint32_t OrderLine::inserted_npage;
pthread_mutex_t OrderLine::insert_lock;
TPCC_PAGE_LIST *OrderLine::first_appended_page[MAX_WORKER_THREAD];
TPCC_PAGE_LIST *OrderLine::last_appended_page[MAX_WORKER_THREAD];
pthread_rwlock_t *OrderLine::locks;
std::set<uint32_t> OrderLine::my_lock_table[32];
