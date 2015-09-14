#ifndef _tpcc_util
#define _tpcc_util

#include <vector>
#include <string>

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
  Permutation(int min, int max);
  void init(int min, int max);
  int next();
};

#endif
