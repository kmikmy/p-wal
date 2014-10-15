for i in `seq 1 10`
do
    g++ -g -O2 -std=c++0x -lpthread -o ../logger.o -c  ../logger.cpp  -DNUM_GROUP_COMMIT=${i}
    g++ -g -O2 -std=c++0x -lpthread -o ../queue_mgr.o -c -DBATCH_TEST ../queue_mgr.cpp
    g++ -g -O2 -std=c++0x -lpthread -o ../aries.exe ../main.o ../transaction.o ../system.o ../logger.o ../update.o ../global.o ../buffer.o ../queue_mgr.o
    ./bench_hdd_batch_100000.sh
    mv hdd.csv hdd_batch_group${i}.csv
done



