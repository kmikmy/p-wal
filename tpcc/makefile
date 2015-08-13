# Makefile
program = tpcc_test
objs = tpcc_test.o
CC = g++
CFLAGS = -g -Wall -std=c++0x -lpthread -O0 -gstabs+

all: tpcc_test

.SUFFIXES: .cpp .o

.cpp.o:	
	$(CC) $(CFLAGS) -MMD -MP -c $<

.PHONY: clean
clean:
	rm -rf  $(program) $(objs) *~ *.exe *.o *.d

tpcc_test: $(objs)
	$(CC) $(CFLAGS) -o $(program).exe $^

create_tables: create_tables.o tpcc_util.o
	$(CC) $(CFLAGS) -o create_tables.exe $^

load_tables: load_tables.o
	$(CC) $(CFLAGS) -o load_tables.exe $^

new-order: new-order.o tpcc_util.o tpcc_table.o ../transaction.o
	$(CC) $(CFLAGS) -o new-order.exe $^