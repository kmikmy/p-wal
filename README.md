
# p-wal


## Install

```
$ cd p-wal
$ make
$ cd tool/
$ make
$ cd ../
$ export ARIES_HOME=`pwd`
```

## Initialization

(Naive)

```
$ tool/write_pages.exe
$ tool/init.exe
```

(P-WAL)

```
$ tool/wwrite_pages.exe
$ tool/init_fio.exe
```


## Execution

```
./aries num_trans num_threads num_group_commit
```

## data/

- system.dat: system log
- page.dat: pages data

## script/
- benchmark script

## tool/
- init.exe: clear ../data/system.dat and /work/kamiya/log.dat
- init_fio.exe: clear ../data/system.dat and /dev/fioa
- write_pages.exe: write to ../data/pages.dat
- read_pages.exe: read pages in ../data/pages.dat
- read_logs.exe: read centralized logs in /work/kamiya/log.dat
- read_fio.exe: read distributed logs in /dev/fioa
