#include "../ARIES.h"
#include <iostream>
#include <cstdlib>

using namespace std;

static void read_master_record();

int main(){
  read_master_record();
}

static void 
read_master_record(){
  int system_fd;
  if( (system_fd = open("../data/system.dat", O_RDWR | O_SYNC | O_CREAT)) == -1 ){
    perror("open");
    exit(1);
  }

  MasterRecord master_record;
  if( read(system_fd, &master_record, sizeof(MasterRecord)) == -1 ){
    perror("read");
    exit(1);
  }

  cout << "xid is " << master_record.system_xid << endl;
  cout << "last_exit is " << std::boolalpha << master_record.last_exit << endl;
}
