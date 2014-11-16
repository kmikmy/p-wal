#include "../ARIES.h"
#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;

#define LOG_OFFSET (1073741824)
#ifndef FIO
#define NUM_MAX_LOGFILE 1
static const char* log_path = "/dev/fioa";
#else
#define NUM_MAX_LOGFILE 50
static const char* log_path = "/dev/fioa";
//static const char* log_path = "/dev/shm/kamiya/log.dat";
#endif


std::ostream& operator<<( std::ostream& os, OP_TYPE& opt){
  switch(opt){
  case NONE: os << "NONE"; break;
  case INC: os << "INC"; break;
  case DEC: os << "DEC"; break;
  case SUBST: os << "SUBST"; break;
  }

  return os;
}

std::ostream& operator<<( std::ostream& os, LOG_TYPE& type){
  //  UPDATE, COMPENSATION, PREPARE, END, OSfile_return, BEGIN };
  switch(type){
  case UPDATE: os << "UPDATE"; break;
  case COMPENSATION: os << "COMPENSATION"; break;
  case PREPARE: os << "PREPARE"; break;
  case BEGIN: os << "BEGIN"; break;
  case END: os << "END"; break;
  case OSfile_return: os << "OSfile_return"; break;

  }

  return os;
}

int main(){
  int fd;
  uint32_t total=0;

  if( (fd = open(log_path,  O_RDONLY )) == -1){
    perror("open");
    exit(1);
  }

  for(int i=0;i<NUM_MAX_LOGFILE;i++){

    off_t base=(off_t)i*LOG_OFFSET;

    lseek(fd, base, SEEK_SET);
    LogHeader lh;
    if(read(fd, &lh, sizeof(LogHeader)) == -1){
      perror("read"); exit(1);
    }
    if(lh.count == 0) continue;

#ifdef FIO
    printf("###   LogFile(%d)   ###\n",i);
#endif

    cout << "the number of logs is " << lh.count << "" << endl;  
    total+=lh.count;
    
    for(unsigned i=0;i<lh.count;i++){
      Log log;
      int len;
      if((len = read(fd, &log, sizeof(Log))) == -1){
	perror("read"); exit(1);
      }    
      if(len == 0) break;
      
      cout << "Log[" << log.LSN << ":" << log.Offset << "]: TransID=" << log.TransID << ", file_id=" << log.file_id << ", Type=" << log.Type;
      
      if(log.Type != BEGIN && log.Type != END)
	cout << ", PrevLSN=" << log.PrevLSN << ", PrevOffset=" << log.PrevOffset << ", UndoNxtLSN=" << log.UndoNxtLSN << ", UndoNxtOffset=" << log.UndoNxtOffset << ", PageID=" << log.PageID << ", before=" << log.before << ", after=" << log.after; // << ", op.op_type=" << log.op.op_type << ", op.amount=" << log.op.amount;


      cout << endl;
    }
  }

  cout << "# total log is " << total << endl;
  return 0;
}
