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

static FieldType
getFieldType(std::string s)
{
  if(s == "int"){
    return TYPE_INT;
  } else if(s == "double"){
    return TYPE_DOUBLE;
  } else if(s == "char"){
    return TYPE_CHAR;
  } else if(s == "uint32_t"){
    return TYPE_UINT32;
  } else if(s == "uint64_t"){
    return TYPE_UINT64;
  } else {
    return TYPE_NONE;
  }
}

static size_t
getTypeSize(std::string s)
{
  if(s == "int"){
    return sizeof(int);
  } else if(s == "double"){
    return sizeof(double);
  } else if(s == "char"){
    return sizeof(char);
  } else if(s == "uint32_t"){
    return sizeof(uint32_t);
  } else if(s == "uint64_t"){
    return sizeof(uint64_t);
  } else {
    return 0;
  }
}


static char*
chomp( char* str )
{
  int l = strlen( str );
  if( l > 0 && str[l-1] == '\n' )
    {
      str[l-1] = '\0';
    }
  return str;
}

static void
loadSchema(const char *fname)
{
  std::string sname = ARIES_HOME;
  sname += SCHEMA_DIR_NAME;
  sname += fname;

  std::regex re("(.+):(int|double|char|uint32_t|uint64_t):?(\\d+)?"); /* (fieldName):(typeName):(length)*/
  std::smatch match;

  std::string tmp;
  std::ifstream ifs(sname.c_str());

  size_t sum = 0;

  TableSchema ts;
  ts.setTableName(fname);

  while(ifs >> tmp){
    if(regex_match(tmp, match, re)){ // match[1] is fieldName, match[2] is typeName, and match[3] is length
      FieldInfo finfo;
      finfo.fieldName = match[1];
      finfo.offset = sum;
      finfo.ftype = getFieldType(match[2]);
      int cnt = atoi(std::string(match[3]).c_str());
      if(cnt == 0) cnt = 1;
      finfo.length = getTypeSize(match[2]) * cnt;
      ts.appendFieldInfo(finfo);
      sum += finfo.length;
    }
  }
  ts.pageSize = sum;
  MasterSchema::appendTableSchema(ts);
}

/* テーブル名を渡して、スキーマ情報を取得&表示する */
void
printSchema(const char *fname)
{
  TableSchema *ts = MasterSchema::getTableSchemaPtr(fname);
  for(auto finfo : ts->fmap){
    std::cout << finfo.second.fieldName << ": offset=" << finfo.second.offset << ", length=" << finfo.second.length << std::endl;
  }
}

/* data/schema/下にある全てのファイルを読みこんで、スキーマ情報をロードする */
void
loadAllSchema()
{
  FILE *fp;
  char buf[BUFSIZ];
  std::vector<std::string> files;
  std::string cmd = "/bin/ls ";
  cmd += ARIES_HOME;
  cmd += SCHEMA_DIR_NAME;

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


TableSchema*
MasterSchema::getTableSchemaPtr(std::string str){
  return &tmap[str];
}

void
MasterSchema::appendTableSchema(TableSchema& ts){
  tmap[ts.getTableName()] = ts;
}


std::map<std::string, TableSchema> MasterSchema::tmap;
