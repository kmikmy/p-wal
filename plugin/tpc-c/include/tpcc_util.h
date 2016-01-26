#ifndef _tpcc_util
#define _tpcc_util

#include <vector>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include "tpcc_page.h"
#include "../../../include/util.h"

extern int W;

/* return [x..y] */
int uniform(int x, int y);

/* return Non Uniform Random value*/
int NURand(int A, int x, int y,int C);

std::string chomp(std::string str);
void gen_c_last(char *c_last, int n);
void gen_rand_astring(char *str, int minlen, int maxlen);
void gen_rand_nstring(char *str, int minlen, int maxlen);
void gen_rand_zip(char *zip);
void gen_date_and_time(char *date_and_time);

/* min >= 8*/
void gen_rand_astring_with_original(char *str, int min, int max, int percentile);

/* return [min/(10^precision) .. max/(10^precision)] */
void gen_rand_decimal(double *d, int min, int max, int precision);

class Permutation{
private:
  std::vector<int> v;
public:
  Permutation(int min, int max); // include max
  void init(int min, int max);
  int next();
};

class Constant{
public:
  int c_for_c_last;
  int c_for_c_id;
  int c_for_ol_i_id;


  static int c_load;

  Constant(){
    /* validate C-Run by using this value(C-Load) */
    int c_run, c_delta;

    if(c_load == -1){
      FD fd;
      fd.open(CLOADFILENAME, O_RDONLY);
      read(fd.fd(), &c_load, sizeof(int));
    }

    do{
      c_run = uniform(0, 255);
      c_delta = abs(c_run - c_load);
    }while( !(65 <= c_delta && c_delta <= 119 && c_delta != 96 && c_delta != 112) );

    c_for_c_last = c_run;
    c_for_c_id = uniform(0, 1023);
    c_for_ol_i_id = uniform(0, 8191);
  }
};

#endif
