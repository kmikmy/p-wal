#include "tpcc.h"
#include "tpcc_page.h"
#include "tpcc_table.h"
#include <cstdlib>

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
TPCC_PAGE_LIST *Warehouse::first_appended_page;
TPCC_PAGE_LIST *Warehouse::last_appended_page;

uint32_t District::npage;
uint32_t District::loaded_npage;
uint32_t District::inserted_npage;
TPCC_PAGE_LIST *District::first_appended_page;
TPCC_PAGE_LIST *District::last_appended_page;

uint32_t Customer::npage;
uint32_t Customer::loaded_npage;
uint32_t Customer::inserted_npage;
TPCC_PAGE_LIST *Customer::first_appended_page;
TPCC_PAGE_LIST *Customer::last_appended_page;

uint32_t NewOrder::npage;
uint32_t NewOrder::loaded_npage;
uint32_t NewOrder::inserted_npage;
TPCC_PAGE_LIST *NewOrder::first_appended_page;
TPCC_PAGE_LIST *NewOrder::last_appended_page;

uint32_t Order::npage;
uint32_t Order::loaded_npage;
uint32_t Order::inserted_npage;
TPCC_PAGE_LIST *Order::first_appended_page;
TPCC_PAGE_LIST *Order::last_appended_page;

uint32_t Item::npage;
uint32_t Item::loaded_npage;
uint32_t Item::inserted_npage;
TPCC_PAGE_LIST *Item::first_appended_page;
TPCC_PAGE_LIST *Item::last_appended_page;

uint32_t Stock::npage;
uint32_t Stock::loaded_npage;
uint32_t Stock::inserted_npage;
TPCC_PAGE_LIST *Stock::first_appended_page;
TPCC_PAGE_LIST *Stock::last_appended_page;

uint32_t OrderLine::npage;
uint32_t OrderLine::loaded_npage;
uint32_t OrderLine::inserted_npage;
TPCC_PAGE_LIST *OrderLine::first_appended_page;
TPCC_PAGE_LIST *OrderLine::last_appended_page;

