#include "include/ARIES.h"
#include "include/cmdline.h"
#include "plugin/tpc-c/include/tpcc_util.h"
#include <cstdlib>
#include <iostream>
#include <pthread.h>
#include <sys/time.h>

enum Mode { NORMAL_EXIT=0, T_START=1, FAILURE=2, EACH_OPERATION };
using namespace std;

extern void batchStartTransaction(int);
extern pthread_t genProducerThread(int _ntrans, int _nqueue);
extern void genWorkerThread(int _nthread);
extern void eachOperationMode(int file_id);

extern MasterRecord master_record;
extern TransTable trans_table;
extern char *ARIES_HOME;

extern int W;

static double
getDiffTimeSec(struct timeval begin, struct timeval end){
  double Diff = (end.tv_sec*1000*1000+end.tv_usec) - (begin.tv_sec*1000*1000+begin.tv_usec);
  return Diff / 1000. / 1000. ;
}

static void
printTPS(struct timeval begin, struct timeval end, uint64_t xact_num){
  double time_sec = getDiffTimeSec(begin, end);
  cout << xact_num / time_sec << endl;
}

std::istream&
operator>>( std::istream& is, Mode& i )
{
  int tmp;
  if ( is >> tmp )
    i = static_cast<Mode>( tmp ) ;
  return is ;
}

static void
fixedThreadMode(int n,int nthread)
{
  struct timeval begin, end;

  ARIES_SYSTEM::dbInit(nthread);
  /*
    gen_producer_thread()とgen_worker_thread()の呼び出し順は固定。
    gen_worker_thread()の中でworkerスレッドに対するpthread_join()が呼び出されているため。
    gen_producer_thread()の中で、トランザクションキューが生成される
  */
  gettimeofday(&begin, NULL);

  pthread_t th = genProducerThread(n, nthread);
  genWorkerThread(nthread);
  pthread_join(th, NULL);
  gettimeofday(&end, NULL);

  printTPS(begin, end, n);

  ARIES_SYSTEM::normalExit();
}

/* ログを書き込むファイルIDを引数に受け取る */
static
void interactMode(int file_id)
{
  ARIES_SYSTEM::dbInit(1);

#ifdef DEBUG
  cout << "DB CONNECTTED" << endl;
  cout << "The number of transaction is " << trans_table.tt_header.tnum << endl;

#endif
  cout << "system_xid is " << ARIES_SYSTEM::master_record.system_xid << endl;

  Logger::init();

  Mode mode;
  do {
    cout << "[NORMAL EXIT]:0, [Trasaction Start]:1, [FAILURE]:2, [EACH_OPERATION]:3 ? " ;
    cin >> mode;

    switch (mode){
    case T_START: batchStartTransaction(1); break;
    case NORMAL_EXIT: ARIES_SYSTEM::normalExit(); exit(0);
    case FAILURE: ARIES_SYSTEM::abnormalExit(); exit(0);
    case EACH_OPERATION: eachOperationMode(file_id); break;
    default: cout << "The mode isn't exist: " << mode << endl;
    }
  } while(mode);
}

int
main(int argc, char *argv[])
{
  ARIES_HOME =  getenv("ARIES_HOME");

  if(argc == 1){
    interactMode(0);
  }
  else if(argc == 2){
    int file_id = atoi(argv[1]);
    interactMode(file_id);
  }
  else if(argc == 3){
    cout << "usage: ./aries num_trans num_threads num_group_commit" << endl;
  }
  else if(argc >= 4){
    uint32_t nxact, nthread, ngroup;
    cmdline::parser p;

    p.add<int>("scale-factor", 'w', "( 1 - 32 )", false, 1, cmdline::range(1, 32));
    p.add<int>("ntransaction", 'x', "( 1 - 100000 )", false, 1, cmdline::range(1, 100000));
    p.add<int>("nthread", 't', "( 1 - 32 )", false, 1, cmdline::range(1, 32));
    p.add<int>("ngroup", 'g', "( 1 - 50 )", false, 1, cmdline::range(1, 50));

    p.parse_check(argc, argv);

    if(p.exist("scale-factor")){
      W = p.get<int>("scale-factor");
    } else {
      W = 1;
    }

    if(p.exist("nthread")){
      nthread = p.get<int>("nthread");
    } else {
      nthread = 1;
    }

    if(p.exist("ntransaction")){
      nxact = p.get<int>("ntransaction");
    } else {
      nxact = 1;
    }

    if(p.exist("ngroup")){
      ngroup = p.get<int>("ngroup");
    } else {
      ngroup = 1;
    }
    Logger::setNumGroupCommit(ngroup);

    //    cout << nxact << ":" << nthread << ":" << ngroup << ":" << W << endl;

    fixedThreadMode(nxact, nthread);
  }
  return 0;
}
