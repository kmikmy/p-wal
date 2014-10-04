# Makefile
program = aries
objs = main.o transaction.o system.o logger.o update.o global.o buffer.o queue_mgr.o
CC = g++
CFLAGS = -g -Wall -O2 -std=c++0x -lpthread

all: aries aries_fio

.SUFFIXES: .cpp .o

.cpp.o:	
	$(CC) $(CFLAGS) -MMD -MP -c $<

.PHONY: clean
clean:
	rm -rf  $(program) $(objs) *~ *.exe

aries: $(objs)
	$(CC) $(CFLAGS) -c logger.cpp 
	$(CC) $(CFLAGS) -c system.cpp 
	$(CC) $(CFLAGS) -o $(program).exe $^

aries_fio: $(objs)
	$(CC) $(CFLAGS) -DFIO -c logger.cpp 
	$(CC) $(CFLAGS) -DFIO -c system.cpp 
	$(CC) $(CFLAGS) -o $(program)_fio.exe $(objs)

