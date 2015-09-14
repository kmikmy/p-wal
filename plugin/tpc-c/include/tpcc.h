/*
 * テーブルの定義はcreate_tableとload_tableに使われている。
 * new-order.cppではtpcc_table.hを参照しているのでテーブルの定義は使われていないが、
 * スケールファクターはここで定義しているので読み込む必要がある。
 */

#ifndef _tpc
#define _tpc

#include "tpcc_util.h"
#include "tpcc_table.h"
#include "tpcc_page.h"
#include "workload.h"
#include "debug.h"

extern int W;

/* #ifndef W */
/* #define W 1 // Scale Factor */
/* #endif */


#endif
