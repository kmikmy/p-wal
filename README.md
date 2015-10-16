# p-wal

## 初期化

(Naive方式)
```
$ tool/write_pages.exe
$ tool/init.exe
```

(P-WAL方式)
```
$ tool/wwrite_pages.exe
$ tool/init_fio.exe
```


## 実行方法

init.exeとwrite_pages.exeを実行する.
その後はルート階層でmakeして出来上がる./aries.exe(HDD)、あるいは./aries_fio.exe(ioDrive)を実行する.

## その他
## data/: システムログやページなどの書き込むデータが集まっている.
-ログはHDDの場合は/work下にあり, ioDriveの場合は/dev/fioaに直接書き込まれる.
-script/: 測定用のスクリプトが入っている

## tool/: ツール群を含んでいる。
-init.exe - ../data/system.dat(xidと前回の終了ステータス), /work/kamiya/log.datをクリア
-init_fio.exe - ../data/system.dat(xidと前回の終了ステータス), /dev/fioaのログをクリア
-write_pages.exe - ../data/pages.datにデータを書き込む
-read_pages.exe - ../data/pages.datのデータを読み込む
-read_logs.exe - /work/kamiya/log.datのログを読み込む
