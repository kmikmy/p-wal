///
/// @file util.h
/// including declaration of FD and MyExeption
///

#pragma once

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string>
#include <exception>
#include <iostream>

class FD
{
 public:

  FD(): fd_(-1){}
  FD(const std::string& filepath, int o_flag, mode_t mode = 0644) : fd_(-1){
	open(filepath, o_flag, mode);
  }
  ~FD(){ close(); }

  int fd() const { return fd_; }

  void open(const std::string& filepath, int o_flag, mode_t mode = 0644);
  void write(const void *ptr, size_t size);
  ssize_t read(void *ptr, size_t size);
  void close();

 private:
  int fd_;
  FD(const FD&);
  void operator=(const FD&);
};

class MyException : std::exception
{
 public:
  explicit MyException(const std::string& cause1,
					   const std::string& cause2 = "",
					   const std::string& cause3 = "") : cause_(cause1+' '+cause2+' '+cause3){}
  ~MyException() throw(){}

  void show();
  const char* what() const throw() {
	return cause_.c_str();
  }

 private:
  std::string cause_;
};
