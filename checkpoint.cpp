/* 現在のトランザクションマネージャにはチェックポイントは実装されていない */

#include "ARIES.h"
#include <iostream>

uint32_t count; // ログに書き込んだ回数%CHKP_INT == 0のときにchkpがとられる

static const mode_t permission = 0777;
extern TransTable trans_table;

void analysis(){
  
}

void begin_chkpoint(){
  Logger::logw

  int fd = open(Logger::logpath, O_CREATE | O_WRONLY, permission);
  if(fd == -1){
    perror("open");
    exit(1);
  }

  write(fd, trans_table.tt_header , sizeof(TT_Header));

  T_ENTRY *e = trans_table.t_entry;
  while(e != NULL){
    write(fd, *(e->trans), sizeof(Transaction));
    e = e->nxt;
  }
  
  T_ENTRY *e = trans_table.t_entry;
  while(e != NULL){
    write(fd, *(e->trans), sizeof(Transaction));
    e = e->nxt;
  }

  close(chkp_fd);

}

void end_chkpoint(){
}
