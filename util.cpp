///
/// @file util.cpp
/// including definition of FD and MyExeption
///

#include "include/util.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <exception>
#include <iostream>

FD::FD(): fd_(-1){}

FD::FD(const std::string& filepath, int o_flag, mode_t mode)
{
  open(filepath, o_flag, mode);
}

FD::~FD()
{
  close()
}

void FD::open(const std::string& filepath, int o_flag, mode_t mode)
{
  fd_ = ::open(filepath.c_str(), o_flag, mode);
  if(fd_ == -1){
	throw MyException("FD", "open");
  }
}

void FD::close()
{
  if(fd_ != -1){
	::close(fd_); // ほんとは返り値をチェックすべき
	fd_ = -1;
  }
}

MyException::MyException(const std::string& cause1, const std::string& cause2, const std::string& cause3) throw()
{
  cause1_ = cause1;
  cause2_ = cause2;
  cause3_ = cause3;
}

MyException::~MyException() throw(){}

void MyException::show()
{
  std::cerr << "exception: " <<  cause1_ << " " << cause2_ << " " << cause3_ << std::endl;
}
