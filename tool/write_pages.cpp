#include "../ARIES.h"
#include <iostream>
#include <ctime>
#include <cstdlib>
#include <unistd.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;

int main(){
  int fd;

  srand(21);

  char *ARIES_HOME =  getenv("ARIES_HOME");
  string page_filename = ARIES_HOME;
  page_filename += "/data/pages.dat";

  if( (fd = open(page_filename.c_str(), O_TRUNC | O_CREAT | O_WRONLY, 0666 )) == -1){
    perror("open");
    exit(1);
  }
  
  for(int i=0;i<PAGE_N;i++){
    Page p;
    p.pageID = i;
    p.value = 0;
    p.page_LSN = 0;
    if( -1 == write(fd, &p, sizeof(Page))){
      perror("write"); exit(1);
    }    
  }
  
  cout << "Success writing " << PAGE_N << " pages" << endl;
  return 0;
}
