#include <iostream>
#include <fstream>
#include <string>
#include <regex>
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "include/schema.h"
#include "include/ARIES.h"
#include "include/debug.h"

extern char *ARIES_HOME;

static FType
getFType(string s){
  if(s == "int"){
    return TYPE_INT;
  } else if(s == "double"){
    return TYPE_DOUBLE;
  } else if(s == "char"){
    return TYPE_CHAR;
  } else {
    return TYPE_NONE;
  }
}

static size_t
getFTypeSize(string s){
  if(s == "int"){
    return sizeof(int);
  } else if(s == "double"){
    return sizeof(double);
  } else if(s == "char"){
    return sizeof(char);
  } else {
    return 0;
  }
}


static char*
chomp( char* str ){
  int l = strlen( str );
  if( l > 0 && str[l-1] == '\n' )
    {
      str[l-1] = '\0';
    }
  return str;
}

static void
loadSchema(const char *fname){
  string sname = ARIES_HOME;
  sname += SCHEMA_DIR_NAME;
  sname += fname;

  regex re("(.+):(int|double|char):(\\d+)?"); /* (fieldName):(typeName):(length)*/
  std::smatch match;

  string tmp;
  std::ifstream ifs(sname.c_str());

  size_t sum = 0;

  TSchema ts;
  ts.setTableName(fname);

  while(ifs >> tmp){
    if(regex_match(tmp, match, re)){ // match[1] is fieldName, match[2] is typeName, and match[3] is length
      FInfo finfo;
      finfo.fieldName = match[1];
      finfo.offset = sum;
      finfo.ftype = getFType(match[2]);
      int cnt = atoi(string(match[3]).c_str());
      if(cnt == 0) cnt = 1;
      finfo.length = getFTypeSize(match[2]) * cnt;
      ts.appendFieldInfo(finfo);
      sum += finfo.length;
    }
  }
  ts.pageSize = sum;
  MasterSchema::appendTableSchema(ts);
}

/* テーブル名を渡して、スキーマ情報を取得&表示する */
void
printSchema(const char *fname){
  TSchema *ts = MasterSchema::getTableSchemaPtr(fname);
  for(auto finfo : ts->fmap){
    cout << finfo.second.fieldName << ": offset=" << finfo.second.offset << ", length=" << finfo.second.length << endl;
  }
}

/* data/schema/下にある全てのファイルを読みこんで、スキーマ情報をロードする */
void
loadAllSchema(){
  FILE *fp;
  char buf[BUFSIZ];
  vector<string> files;
  string cmd = "/bin/ls ";
  cmd += ARIES_HOME;
  cmd += SCHEMA_DIR_NAME;

  cout << cmd.c_str() << endl;

  if( (fp = popen(cmd.c_str(), "r")) == NULL ){
    PERR("popen");
  }

  while(fgets(buf, BUFSIZ, fp) != NULL ){
    files.push_back(chomp(buf));
  }
  pclose(fp);

  for(auto filename : files){
    //    cout << filename << " is loaded." << endl;
    loadSchema(filename.c_str());
    //    printSchema(filename.c_str());
  }
}


TSchema*
MasterSchema::getTableSchemaPtr(string str){
  return &tmap[str];
}

void
MasterSchema::appendTableSchema(TSchema& ts){
  tmap[ts.getTableName()] = ts;
}


map<string, TSchema> MasterSchema::tmap;

