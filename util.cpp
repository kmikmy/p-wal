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

void FD::write(const void *ptr, size_t size){
  ssize_t ret = ::write(fd_, ptr, size); // ほんとは全て書いたかをチェックする
  if(ret == -1){
	throw MyException("FD", "write");
  }
}

void FD::close()
{
  if(fd_ != -1){
	::close(fd_); // ほんとは返り値をチェックすべき
	fd_ = -1;
  }
}

MyException::~MyException() throw(){}

void MyException::show()
{
  std::cerr << "exception: " <<  cause1_ << " " << cause2_ << " " << cause3_ << std::endl;
}
