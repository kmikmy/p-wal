#include <map>
#include <unistd.h>
#include <cstdio>
#include <map>
#include <string>
#include <iostream>


using namespace std;

void func1() { cout << "func1" << endl; }
void func2() { cout << "func2" << endl; }

typedef map<string, void(*)()> funcs_type;

int main()
{


  funcs_type funcs;
  funcs.insert(make_pair("abc", func1));
  funcs.insert(make_pair("xyz", func2));

  string str;
  str = "abc";

  funcs_type::iterator it = funcs.find(str);
  if (it != funcs.end())
    it->second();
  else
    cout << "not found" << endl;

  return 0;
}
