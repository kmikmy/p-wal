#include <iostream>
#include <cstdlib>
#include "../include/ARIES.h"

using namespace std;

#define LOG_OFFSET (1073741824)
#ifndef FIO
#define NUM_MAX_LOGFILE 1
//static const char* log_path = "/dev/fioa";
static const char* log_path = "/dev/fioa";
#else
#define NUM_MAX_LOGFILE 50
static const char* log_path = "/dev/fioa";
#endif


void init();

int main(){
  init();
}

void init(){

  int fd;
  MasterRecord master_record;
  char *ARIES_HOME =  getenv("ARIES_HOME");
  string system_filename = ARIES_HOME;
  cout << system_filename << endl;
  system_filename += "/data/system.dat";
 
  if( (fd = open(system_filename.c_str(), O_RDWR | O_SYNC | O_CREAT, 0666 )) == -1 ){
    perror("open");
    exit(1);
  }
  int len1;

  
  if( (len1 = read(fd, &master_record, sizeof(MasterRecord))) == -1 ){
    perror("read");
    exit(1);
  }

  cout << "[before init]" << endl;
  cout << "chkp:" <<  master_record.mr_chkp << ", xid:" << master_record.system_xid << ", last_exit:" << std::boolalpha << master_record.last_exit << endl;

  master_record.mr_chkp=0;
  master_record.system_xid=1;
  master_record.system_last_lsn=1; // fetch_and_addを使う
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
  if( (fd = open(log_path, O_WRONLY | O_CREAT, 0666)) == -1 ){  
    perror("open");
    exit(1);
  }
  cout << log_path << endl;
  int log_num = 1;
#endif
#ifdef FIO
  if( (fd = open(log_path, O_WRONLY)) == -1 ){  
    perror("open");
    exit(1);
  }
  int log_num = 32;
#endif

    uint64_t base = 0;
    for(int i=0;i<log_num;i++,base += LOG_OFFSET){
#ifdef FIO
    printf("###   LogFile #%d is cleared   ###\n",i);
#endif

    lseek(fd, base, SEEK_SET);

    LogHeader* lh;
    if ((posix_memalign((void **) &lh, 512, sizeof(LogHeader))) != 0)
      {
        fprintf(stderr, "posix_memalign failed\n");
	exit(1);
      }
    if(-1 == write(fd, lh, sizeof(LogHeader))){
      perror("write");
      exit(1);
    }
  }
 
  close(fd);
}
