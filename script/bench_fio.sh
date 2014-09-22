echo "# thread,first time,second time,third time," > fio.txt
for i in `seq 1 7`
do
echo -n "${i}," >> fio.txt
    for j in `seq 1 3`
    do
    sleep 1
    sudo /home/kamiya/hpcs/aries/tool/init_fio.exe > /dev/null
    (/usr/bin/time -f "%e" sudo /home/kamiya/hpcs/aries/aries_fio.exe 10000 $i) 2>&1 | tr "\n" "," >> fio.txt
    done
    echo "" >> fio.txt
done


