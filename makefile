# Makefile
# $@: ターゲット名(aries, aries_batch, aries_fio or aries_fio_batch)
# $^: 全ての依存するファイル名
# $<: 最初の依存するファイル名(１個１個コンパイルするときにつかう)
program = aries
objs = main.o schema.o transaction.o system.o logger.o update.o global.o buffer.o queue_mgr.o dpt.o util.o plugin/tpc-c/new-order.o plugin/tpc-c/tpcc_table.o plugin/tpc-c/tpcc_util.o # recovery.o
srcs = $(objs:%.o=%.c)
CC = g++
CFLAGS = -g -Wall -std=c++11 -lpthread -O2 #-lprofiler

all: aries aries_batch aries_fio aries_fio_batch

.SUFFIXES: .cpp .o

.cpp.o:
	$(CC) $(CFLAGS) -MMD -MP -c $< -o $@

.PHONY: clean
clean:
	rm -rf  $(program) $(objs) *.d *~ *.exe

aries: $(objs)
	$(CC) $(CFLAGS) -c logger.cpp
	$(CC) $(CFLAGS) -c system.cpp
#	$(CC) $(CFLAGS) -c recovery.cpp
	$(CC) $(CFLAGS) -c queue_mgr.cpp
	$(CC) $(CFLAGS) -o $(program).exe $^

aries_batch: $(objs)
	$(CC) $(CFLAGS) -c -DBATCH_TEST queue_mgr.cpp
	$(CC) $(CFLAGS) -o $(program)_batch.exe $^

aries_fio: $(objs)
	$(CC) $(CFLAGS) -DFIO -c logger.cpp
	$(CC) $(CFLAGS) -DFIO -c system.cpp
#	$(CC) $(CFLAGS) -DFIO -c recovery.cpp
	$(CC) $(CFLAGS) -c queue_mgr.cpp
	$(CC) $(CFLAGS) -o $(program)_fio.exe $(objs)

aries_fio_batch: $(objs)
	$(CC) $(CFLAGS) -c -DBATCH_TEST queue_mgr.cpp
	$(CC) $(CFLAGS) -o $(program)_fio_batch.exe $(objs)
