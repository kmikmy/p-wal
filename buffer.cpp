#include "ARIES.h"

using namespace std;

extern PageBufferEntry page_table[PAGE_N];

bool
is_page_fixed(int page_id){
  return page_table[page_id].fixed_flag;
}

void
page_fix(int page_id){
  PageBufferEntry *pbuf = &page_table[page_id];
  pbuf->fixed_flag = true;
    
  int fd;
  if( (fd = open("data/pages.dat", O_CREAT | O_RDONLY )) == -1){
    perror("open");
    exit(1);
  }
  if( -1 == read(fd, &pbuf->page, sizeof(Page))){
    perror("read"); exit(1);
  } 
  pbuf->page_id = page_id;
}
