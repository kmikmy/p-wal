#include <random>
#include <cstring>
#include "include/tpcc.h"


int W = 0;

/* return [x..y] */
int
uniform(int x, int y){
  return (rand()/(RAND_MAX+1.0))*(y-x+1)+x;
}

/* return Non Uniform Random value*/
int
NURand(int A, int x, int y, int C){
  return (((uniform(0,A) | uniform(x,y)) + C) % (y-x+1)) + x;
}

std::string
chomp(std::string str){
  std::string::size_type pos = str.find_last_not_of("\n \t");
  if(pos != std::string::npos)
    str = str.substr(0, pos+1);
  return str;
}

Permutation::Permutation(int min, int max){
  for(int i=min; i<=max; i++){
    v.push_back(i);
  }
}

void
Permutation::init(int min, int max){
  v.clear();
  for(int i=min; i<=max; i++){
    v.push_back(i);
  }
}

int
Permutation::next(){
  int pos, ret;
  if(v.size() == 0){
    PERR("Elements in vector does not exist.");
  }
  pos = uniform(0, v.size()-1);
  ret = v.at(pos);
  v.erase(v.begin() + pos);

  return ret;
}

void
gen_c_last(char *c_last, int n){
  c_last[0] = '\0';
  char syllables[10][6]={"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
  strncat(c_last, syllables[n/100],6);
  strncat(c_last, syllables[(n/10)%10],6);
  strncat(c_last, syllables[n%10],6);
}

void
gen_rand_astring(char *str, int minlen, int maxlen){
  int i;
  int len = uniform(minlen, maxlen);
  int tmp;
  
  for(i=0; i<len; i++){
    tmp = rand() % 62;
    if(tmp < 26){
      str[i] = 'a' +  tmp;
    } else if(tmp < 52) {
      str[i] = 'A' + (tmp-26);
    } else{
      str[i] = '0' + (tmp-52);
    }
  }
  str[i] = '\0';
}

void
gen_rand_nstring(char *str, int minlen, int maxlen){
  int i;
  int len = uniform(minlen, maxlen);
  for(i=0; i<len; i++){
    str[i] = '0' + (rand() % 10 );
  }
  str[i] = '\0';
}

void
gen_rand_zip(char *zip){
  gen_rand_nstring(zip, 4, 4);
  strncat(&zip[4], "11111", 6); // include '\0'
}


void
gen_date_and_time(char *date_and_time){
  time_t t;
  time(&t);
  char *_c_since = ctime(&t);
  std::string str = _c_since;
  str = chomp(str);
  strncpy(date_and_time, str.c_str(), strlen(str.c_str())+1); // include '\0'
}

/* min >= 8*/
void
gen_rand_astring_with_original(char *str, int min, int max, int percentile){
  bool original_flag = false;
  if(percentile > uniform(0,99)){
    original_flag = true;
  }
  
  int len = uniform(min, max);
  gen_rand_astring(str, len, len);
  
  if(original_flag){
    int insert_pos = uniform(0, len-8);
    strncpy(&str[insert_pos], "ORIGINAL", 8); // ! not include '\0'
  }  
}

/* return [min/(10^precision) .. max/(10^precision)] */
void gen_rand_decimal(double *d, int min, int max, int precision){
  int i;
  *d = uniform(min, max);

  for(i=0;i<precision;i++){
    *d /= 10;
  }
}

