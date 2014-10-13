for i in `seq 1 10`
do
    g++ -g -O2 -std=c++0x -lpthread -o ../queue_mgr.o -c -DBATCH_TEST -DNUM_GROUP_COMMIT=${i} ../queue_mgr.cpp
    g++ -g -Wall -O2 -std=c++0x -lpthread -o aries_fio.exe main.o transaction.o system.o logger.o update.o global.o buffer.o queue_mgr.o
    ./bench_fio_batch.sh
    mv fio.csv fio_batch_group${i}.csv
done



