#include "tpcc.h"


int main(){
  TableWarehouse T;
  T.table_type = WAREHOUSE;
  T.select("A","43");
  T.update(1,"A",1);

  
}
