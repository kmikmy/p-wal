echo "# thread,first time,second time,third time," > fio.csv
for i in `seq 1 7`
do
echo -n "${i}," >> fio.csv
    for j in `seq 1 3`
    do
    sleep 1
    sudo /home/kamiya/hpcs/aries/tool/init_fio.exe > /dev/null
    (/usr/bin/time -f "%e" sudo /home/kamiya/hpcs/aries/aries_fio_batch.exe 10000 $i) 2>&1 | tr "\n" "," >> fio.csv
    done
    echo "" >> fio.csv
done


