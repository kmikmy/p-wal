echo "# thread,first time,second time,third time," > hdd.csv
for i in `seq 1 7`
do
echo -n "${i}," >> hdd.csv
    for j in `seq 1 3`
    do
    sleep 1
    /home/kamiya/hpcs/aries/tool/init.exe > /dev/null
    (/usr/bin/time -f "%e" /home/kamiya/hpcs/aries/aries_batch.exe 10000 $i) 2>&1 | tr "\n" "," >> hdd.csv
    done
    echo "" >> hdd.csv
done


