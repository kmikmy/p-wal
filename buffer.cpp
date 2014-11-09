#include "ARIES.h"
#include "dpt.h"

using namespace std;

extern char* ARIES_HOME;
extern DirtyPageTable dirty_page_table;

BufferControlBlock page_table[PAGE_N];
static int page_fd[MAX_WORKER_THREAD];
static int page_fd_for_flush;
static std::mutex page_mtx;

void 
pbuf_init(){
  int page_fd;
  std::string page_filename = ARIES_HOME;
  page_filename += "/data/pages.dat";
    
  if( (page_fd = open(page_filename.c_str(), O_CREAT | O_RDONLY )) == -1){
    perror("open");
    exit(1);
  }

  for(int i=0;i<PAGE_N;i++){
    pthread_rwlock_init(&page_table[i].lock, NULL);
    page_table[i].page_id=i;
    page_table[i].fixed_count = 0;
    if(-1 == read(page_fd, &page_table[i].page, sizeof(Page))){
      perror("read");
      exit(1);
    }
    page_table[i].readed_flag=true;
    page_table[i].modified_flag=false;
  }
}

bool
is_page_fixed(int page_id){
  return page_table[page_id].fixed_count > 0;
}

void
page_fix(int page_id, int th_id){
  BufferControlBlock *pbuf = &page_table[page_id];
    
  if(!is_page_fixed(page_id)){
    if( page_fd[th_id] == 0 ){
      std::string page_filename = ARIES_HOME;
      page_filename += "/data/pages.dat";
    
      if( (page_fd[th_id] = open(page_filename.c_str(), O_CREAT | O_RDONLY )) == -1){
	perror("open");
	exit(1);
      }
    }

    /* システムを起動してから初めてページを読み込む場合 */
    if(!pbuf->readed_flag){
      pbuf->readed_flag = true;
      pbuf->modified_flag = true; // 現在の実装ではUPDATEしかないので常にmodifyのintentionを持ったfixになる

      //    Pageの読み込み;
      lseek(page_fd[th_id], (off_t)sizeof(Page)*page_id, SEEK_SET);
      if( -1 == read(page_fd[th_id], &pbuf->page, sizeof(Page))){
	perror("read"); exit(1);
      } 
      pbuf->page_id = page_id;
    }


    /* 
       dirty_pages_tableのRecLSN更新
       RecLSN以降にそのページに、Diskに更新が反映されていないUPDATEログがある可能性がある。
       リカバリ時、このページは少なくともRecLSNからredoをしなければない。
    */
    dirty_page_table.add(pbuf->page_id,Logger::read_LSN());
    pbuf->fixed_count=1;
  }
  else{ // 既にfixされているなら
    pbuf->fixed_count++;
  }
}


/* 
   fixされていなくて、modifiedフラグが立っているページのみflushする。
   flushされたページはmodifiedフラグが外される。
   未実装.

*/
void 
flush_page(){
  std::lock_guard<std::mutex> lock(page_mtx);  
  
  if(page_fd_for_flush == 0){
    std::string page_filename = ARIES_HOME;
    page_filename += "/data/pages.dat";
    
    if( (page_fd_for_flush = open(page_filename.c_str(), O_CREAT | O_WRONLY )) == -1){
      perror("open");
      exit(1);
    }
  }

  for(int i=0;i<PAGE_N;i++){
    if(!is_page_fixed(i)) // 正確にはis_page_modifiedにしなければならない。またfixされているページの情報はD.P.Tの内容を見るだけでよいはずだ。
      continue;

    lseek(page_fd_for_flush,sizeof(Page)*page_table[i].page_id, SEEK_SET);
    if( -1 == write(page_fd_for_flush, &page_table[i].page, sizeof(Page))){
      perror("write"); exit(1);
    }

    page_table[i].modified_flag=false;
    page_table[i].fixed_count=0;
    
    // dirty_page_table.erase(dirty_page_table.find(i));
    
  }    
}
