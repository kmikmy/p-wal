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

uint64_t current_offset = 0;
uint64_t current_records = 0;

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
  field_log_body += sizeof(FieldLogHeader);

  memcpy(before, field_log_body, flh->fieldLength);
  memcpy(after , field_log_body + flh->fieldLength, flh->fieldLength);

  cout << "offset: " << flh->fieldOffset << ", length: " << flh->fieldLength <<
    ", before: "  << *(int *)before << ", after: "  << *(int *)after << endl;
}

void
DisplayLogRecordHeader(LogRecordHeader *log){
  cout << "Log[" << log->lsn << ":" << log->offset << "]: TransID=" << log->trans_id << ", file_id=" << log->file_id << ", Type=" << log->type;

  if(log->type != BEGIN && log->type != END)
    cout << ", table_name=" << log->table_name <<", prev_lsn=" << log->prev_lsn << ", prev_offset=" << log->prev_offset << ", undo_nxt_lsn=" << log->undo_nxt_lsn << ", undo_nxt_offset=" << log->undo_nxt_offset << ", page_id=" << log->page_id << ", field_num=" << log->field_num;
  //<< ", before=" << log->before << ", after=" << log->after << ", op.op_type=" << log->op.op_type << ", op.amount=" << log-op.amount;
  cout << endl;
}

void
DisplayLogRecord(LogRecordHeader *lrh, char *log_record_body){
  uint64_t ptr_on_log_record = sizeof(LogRecordHeader);
  DisplayLogRecordHeader(lrh);

  FieldLogHeader field_log_header;
  for(uint32_t i=0; i < lrh->field_num; i++){

    // field_log_headerの値の読み込みがおかしい
    memcpy(&field_log_header, log_record_body + ptr_on_log_record, sizeof(FieldLogHeader));

    DisplayFieldLog(&field_log_header, log_record_body + ptr_on_log_record);

    ptr_on_log_record += (sizeof(FieldLogHeader) + field_log_header.fieldLength * 2);
  }
}

void
DisplayChunk(ChunkLogHeader *clh, char *chunk_buffer){
  uint64_t ptr_on_chunk = sizeof(ChunkLogHeader);

  cout << "chunk_size: " << clh->chunk_size << ", log_record_num: " <<
    clh->log_record_num << endl;

  current_offset += ptr_on_chunk;
  current_records = 0;

  LogRecordHeader lrh;
  for(uint32_t i=0; i < clh->log_record_num; i++){
    current_records++;
    memcpy(&lrh, chunk_buffer+ptr_on_chunk, sizeof(LogRecordHeader));
    std::cout << "[" << current_records << "/" << clh->log_record_num << "]" << "current_offset: " << current_offset << std::endl;
    DisplayLogRecord(&lrh, chunk_buffer+ptr_on_chunk);
    ptr_on_chunk += lrh.total_length;
    current_offset += lrh.total_length;

  }
}

void
DisplayLogs(){
  int fd;
  uint32_t total_log_record_num=0;

  if( (fd = open(log_path,  O_RDONLY)) == -1){
    perror("open");
    exit(1);
  }

  off_t base = 0;
  for(int i=0;i<kNumMaxLogSegment;i++,base+=LOG_OFFSET){
    current_offset = base;
    std::cout << "current_offset: " << current_offset << std::endl;

#ifdef FIO
    printf("\n###   LogFile(%d)   ###\n",i);
#endif

    LogSegmentHeader* lsh = NULL;
    lseek(fd, base, SEEK_SET);
    PosixMemAlignReadOrDie(fd, (char **)&lsh, sizeof(LogSegmentHeader));
    if(lsh->chunk_num == 0) continue;

    cout << "the number of log is " << lsh->log_record_num << "" << endl;
    cout << "the number of chunk is " << lsh->chunk_num << "" << endl;
    cout << "the segment size is " << lsh->segment_size << "" << endl;
    cout << endl;
    total_log_record_num += lsh->log_record_num;

    for(uint32_t i=0; i < lsh->chunk_num; i++){
      char *chunk_buffer = NULL;

      PosixMemAlignReadOrDie(fd, (char **)&chunk_buffer, kFirstReadChunkSize);

      ChunkLogHeader clh;
      memcpy(&clh, chunk_buffer, sizeof(ChunkLogHeader));

      if(clh.chunk_size != kFirstReadChunkSize){
	current_offset = lseek(fd, -kFirstReadChunkSize, SEEK_CUR);
	std::cout << "current_offset: " << current_offset << std::endl;

	free(chunk_buffer);
	PosixMemAlignReadOrDie(fd, &chunk_buffer, clh.chunk_size);
      }

      DisplayChunk(&clh, chunk_buffer);
      free(chunk_buffer);
    }
  }

}

int
main(){
  DisplayLogs();

  return 0;
}
