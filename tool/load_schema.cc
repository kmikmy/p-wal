#include "../include/schema.h"
#include "../include/table.h"
#include "../include/log.h"
#include "../include/debug.h"
#include "../include/query.h"
#include <cstring>
#include <iostream>

char *ARIES_HOME;

int
main(){
  ARIES_HOME = getenv("ARIES_HOME");

  //  Logger::init();


  loadAllSchema();
  //  printSchema("customer");
  printSchema("simple");

  //  loadTable("test");

  for(int i=0;i<10;i++)
  {
    //    begin();
    QueryArg q;
    int before_int = 1;
    int after_int = 2;

    q.before = (char *)&before_int;
    q.after = (char *)&after_int;
    q.field_name = new char[strlen("c_id")+1];
    memcpy(q.field_name, "c_id", strlen("c_id")+1);
    //    update("customer", &q, 1);
    //    end();
  }
}
