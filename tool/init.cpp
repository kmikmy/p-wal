#include <iostream>
#include <cstdlib>
#include "../ARIES.h"

using namespace std;

#ifndef FIO
static const char *log_path = "/work/kamiya/log.dat";
#else
static const char *log_path = "/dev/fioa";
#endif

void init();

int main(){
  init();
}

void init(){

  int fd;
  MasterRecord master_record;

  if( (fd = open("/home/kamiya/hpcs/aries/data/system.dat", O_RDWR | O_SYNC | O_CREAT )) == -1 ){
    perror("open");
    exit(1);
  }
  int len1;

  
  if( (len1 = read(fd, &master_record, sizeof(MasterRecord))) == -1 ){
    perror("read");
    exit(1);
  }

  cout << "[before init]" << endl;
  cout << "chkp:" <<  master_record.mr_chkp << ", xid:" << master_record.system_xid << ", last_exit:" << master_record.last_exit << endl;

  master_record.mr_chkp=0;
  master_record.system_xid=0;
  master_record.last_exit=true;

  cout << "[after init]" << endl;
  cout << "chkp:" <<  master_record.mr_chkp << ", xid:" << master_record.system_xid << ", last_exit:" << master_record.last_exit << endl;

  lseek(fd, 0, SEEK_SET);
  if( -1 == write(fd, &master_record, sizeof(MasterRecord))) {
    perror("write");
    exit(1);
  }

  close(fd);

#ifndef FIO
  if( (fd = open(log_path, O_WRONLY | O_TRUNC | O_CREAT )) == -1 ){  
    perror("open");
    exit(1);
  }
#endif
#ifdef FIO
  if( (fd = open(log_path, O_WRONLY )) == -1 ){  
    perror("open");
    exit(1);
  }
#endif

  LogHeader lh = {0};
  if(-1 == write(fd, &lh, sizeof(lh))){
    perror("write");
    exit(1);
  }

 
  close(fd);
}
