#include "../include/ARIES.h"
#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;

#ifndef FIO
static const int kNumMaxLogSegment=1;
static const char* log_path = "/dev/fioa";
#else
static const int kNumMaxLogSegment=32;
static const char* log_path = "/dev/fioa";
//static const char* log_path = "/dev/shm/kamiya/log.dat";
#endif


const int kFirstReadChunkSize = 512;
const int kMaxFieldLength = 256;

std::ostream& operator<<( std::ostream& os, OP_TYPE& opt){
  switch(opt){
  case NONE: os << "NONE"; break;
  case INC: os << "INC"; break;
  case DEC: os << "DEC"; break;
  case SUBST: os << "SUBST"; break;
  case READ: os << "[READ]"; break;
  }

  return os;
}

std::ostream& operator<<( std::ostream& os, kTableType& tid){
  switch(tid){
  case SIMPLE: os << "SIMPLE"; break;
  case WAREHOUSE: os << "WAREHOUSE"; break;
  case DISTRICT: os << "DISTRICT"; break;
  case CUSTOMER: os << "CUSTOMER"; break;
  case HISTORY: os << "HISTORY"; break;
  case ORDER: os << "ORDER"; break;
  case ORDERLINE: os << "ORDERLINE"; break;
  case NEWORDER:  os << "NEWORDER"; break;
  case ITEM:  os << "ITEM"; break;
  case STOCK:  os << "STOCK"; break;
  }

  return os;
}

std::ostream& operator<<( std::ostream& os, kLogType& type){
  //  UPDATE, COMPENSATION, PREPARE, END, OSfile_return, BEGIN };
  switch(type){
  case UPDATE: os << "UPDATE"; break;
  case COMPENSATION: os << "COMPENSATION"; break;
  case PREPARE: os << "PREPARE"; break;
  case BEGIN: os << "BEGIN"; break;
  case END: os << "END"; break;
  case OSfile_return: os << "OSfile_return"; break;
  case INSERT: os << "INSERT"; break;
  }

  return os;
}

void
PosixMemAlignReadOrDie(int fd, char **chunk_buf_dst, uint64_t read_size){
  if ((posix_memalign((void **) chunk_buf_dst, 512, read_size)) != 0){
    perror("posix_memalign");
    exit(1);
  }
  if(read(fd, *chunk_buf_dst, read_size) == -1){
    perror("read");
    exit(1);
  }
}

void
DisplayFieldLog(FieldLogHeader *flh, char *field_log_body){
  char *before[kMaxFieldLength], *after[kMaxFieldLength];

  memcpy(before, field_log_body, flh->fieldLength);
  memcpy(after , field_log_body + flh->fieldLength, flh->fieldLength);

  cout << "offset: " << flh->fieldOffset << ", length: " << flh->fieldLength <<
    ", before: "  << *(int *)before << ", after: "  << *(int *)after;
}

void
DisplayLogRecordHeader(LogRecordHeader *log){
  cout << "Log[" << log->lsn << ":" << log->offset << "]: TransID=" << log->trans_id << ", file_id=" << log->file_id << ", Type=" << log->type;

  if(log->type != BEGIN && log->type != END)
    cout << ", table_id=" << log->table_id <<", prev_lsn=" << log->prev_lsn << ", prev_offset=" << log->prev_offset << ", undo_nxt_lsn=" << log->undo_nxt_lsn << ", undo_nxt_offset=" << log->undo_nxt_offset << ", page_id=" << log->page_id;
  //<< ", before=" << log->before << ", after=" << log->after << ", op.op_type=" << log->op.op_type << ", op.amount=" << log-op.amount;

  cout << endl;
}

void
DisplayLogRecord(LogRecordHeader *lrh, Log *log){
  uint64_t ptr_on_log_record = 0 + sizeof(LogRecordHeader);
  FieldLogHeader field_log_header;

  DisplayLogRecordHeader(lrh);
  for(uint32_t i=0; i < lrh->field_num; i++){
    char *field_log_body = (char *)log + ptr_on_log_record;
    memcpy(&field_log_header, log + ptr_on_log_record, sizeof(FieldLogHeader));
    DisplayFieldLog(&field_log_header, field_log_body);
  }
}

void
DisplayChunk(ChunkLogHeader *clh, char *chunk_buffer){
  uint64_t ptr_on_chunk = 0 + sizeof(ChunkLogHeader);
  cout << "chunk_size: " << clh->chunk_size << ", log_record_num: " <<
    clh->log_record_num << endl;

  LogRecordHeader lrh;
  for(uint32_t i; i < clh->log_record_num; i++){
    memcpy(&lrh, chunk_buffer+ptr_on_chunk, sizeof(LogRecordHeader));
    DisplayLogRecord(&lrh, (Log *)chunk_buffer+ptr_on_chunk);
    ptr_on_chunk += lrh.total_length;
  }
}

void
DisplayLogs(){
  int fd;
  uint32_t total_log_num=0;

  if( (fd = open(log_path,  O_RDONLY)) == -1){
    perror("open");
    exit(1);
  }

  off_t base = 0;
  for(int i=0;i<kNumMaxLogSegment;i++,base+=LOG_OFFSET){

#ifdef FIO
    printf("\n###   LogFile(%d)   ###\n",i);
#endif

    LogSegmentHeader* lsh = NULL;
    lseek(fd, base, SEEK_SET);
    PosixMemAlignReadOrDie(fd, (char **)&lsh, sizeof(LogSegmentHeader));
    if(lsh->chunk_num == 0) continue;

    cout << "the number of log is " << lsh->log_num << "" << endl;
    total_log_num += lsh->log_num;

    for(uint32_t i=0; i < lsh->chunk_num; i++){
      char *chunk_buffer = NULL;

      PosixMemAlignReadOrDie(fd, (char **)&chunk_buffer, kFirstReadChunkSize);

      ChunkLogHeader clh;
      memcpy(&clh, chunk_buffer, sizeof(ChunkLogHeader));

      if(clh.chunk_size != kFirstReadChunkSize){
	lseek(fd, -sizeof(ChunkLogHeader), SEEK_CUR);
	free(chunk_buffer);
	PosixMemAlignReadOrDie(fd, &chunk_buffer, clh.chunk_size);
      }

      DisplayChunk(&clh, chunk_buffer);
    }
  }

}

int
main(){
  DisplayLogs();

  return 0;
}
