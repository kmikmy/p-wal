#include "../ARIES.h"
#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;

int main(){
  int fd;

  if( (fd = open("/home/kamiya/hpcs/aries/data/pages.dat",  O_RDONLY )) == -1){
    perror("open");
    exit(1);
  }
  
  int n=0;
  for(int i=1;i<=PAGE_N;i++){
    Page p;
    int ret;
    if( -1 == (ret = read(fd, &p, sizeof(Page))) ){
      perror("exit"); exit(1);
    }
    if( ret == 0)
      break;

    n++;
    cout << "page[" << p.pageID << "]: page_LSN=" << p.page_LSN << ", value=" << p.value << endl;
  }
  
  cout << "Success reading " << n << " pages" << endl;
  return 0;
}
