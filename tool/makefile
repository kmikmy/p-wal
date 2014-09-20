# Makefile
CC = g++
CFLAGS = -g -Wall -O2 -std=c++0x -lpthread
PROGS = write_pages read_pages read_master_record read_logs read_fio init init_fio


.SUFFIXES: .cpp .o

.cpp.o:	
	$(CC) $(CFLAGS) -MMD -MP -c $<

.PHONY: clean


# all
all: $(PROGS)


# 共通
write_pages: write_pages.cpp
	$(CC) $(CFLAGS) -o write_pages.exe write_pages.cpp

read_pages: read_pages.cpp
	$(CC) $(CFLAGS) -o read_pages.exe read_pages.cpp

read_master_record: read_master_record.cpp
	$(CC) $(CFLAGS) -o read_master_record.exe read_master_record.cpp


# FusionとHDDで異なる

read_logs: read_logs.cpp
	$(CC) $(CFLAGS) -o read_logs.exe read_logs.cpp
read_fio: read_logs.cpp
	$(CC) $(CFLAGS) -DFIO -o read_fio.exe read_logs.cpp

#read_logs: read_fio.cpp
#	$(CC) $(CFLAGS) -o read_logs.exe read_fio.cpp
#read_fio: read_fio.cpp
#	$(CC) $(CFLAGS) -DFIO -o read_fio.exe read_fio.cpp

init: init.cpp
	$(CC) $(CFLAGS) -o init.exe init.cpp

init_fio: init.cpp
	$(CC) $(CFLAGS) -DFIO -o init_fio.exe init.cpp


# clean
clean:
	rm -rf *.o *~ *.exe

