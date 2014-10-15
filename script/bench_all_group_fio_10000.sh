for i in `seq 1 10`
do
    g++ -g -O2 -std=c++0x -lpthread -o ../logger.o -c  ../logger.cpp  -DFIO  -DNUM_GROUP_COMMIT=${i}
    g++ -g -O2 -std=c++0x -lpthread -o ../queue_mgr.o -c -DBATCH_TEST ../queue_mgr.cpp
    g++ -g -O2 -std=c++0x -lpthread -o ../aries_fio.exe ../main.o ../transaction.o ../system.o ../logger.o ../update.o ../global.o ../buffer.o ../queue_mgr.o
    ./bench_fio_batch_10000.sh
    mv fio.csv fio_batch_group${i}.csv
done



