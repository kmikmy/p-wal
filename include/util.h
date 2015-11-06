///
/// @file util.h
/// including declaration of FD and MyExeption
///

#ifndef _util
#define _util

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <exception>
#include <iostream>

class FD
{
 public:
  FD();
  FD(const std::string& filepath, int o_flag, mode_t mode = 0644);
  virtual ~FD();

  operator int();

  void open(const std::string& filepath, int o_flag, mode_t mode = 0644);

 protected:
  int fd_;
};

class MyException : std::exception
{
 public:
  MyException(const std::string& cause1, const std::string& cause2 = "", const std::string& cause3 = "") throw();
  ~MyException() throw();

  void show();

 protected:
  std::string cause1_;
  std::string cause2_;
  std::string cause3_;
};

#endif
