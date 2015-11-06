///
/// @file util.cpp
/// including definition of FD and MyExeption
///

#include "include/util.h"

void FD::open(const std::string& filepath, int o_flag, mode_t mode)
{
  close();
  fd_ = ::open(filepath.c_str(), o_flag, mode);
  if(fd_ == -1){
	throw MyException("FD", "open");
  }
}

void FD::write(const void *ptr, size_t size)
{
  ssize_t ret = ::write(fd_, ptr, size); // ほんとは全て書いたかをチェックする // ほんとはerrnoをチェックしてEINTRならリトライする
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

void MyException::show()
{
  std::cerr << "exception: " <<  what() << std::endl;
}
