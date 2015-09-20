#ifndef _schema
#define _schema

#include <map>
#include <iostream>
#include <regex>

using std::cout;
using std::endl;
using std::string;
using std::regex;
using std::vector;
using std::map;

#define SCHEMA_DIR_NAME "/data/schema/"

enum FType {TYPE_INT, TYPE_DOUBLE, TYPE_CHAR, TYPE_NONE};

/*
  ##################################### 
  function declaration
  ##################################### 
*/

void loadAllSchema(); // MasterSchemaクラスの静的メンバに全てのスキーマをロードする
void printSchema(const char *fname);
/*
  ##################################### 
  class definition
  ##################################### 
*/

class TableHeader {
 public:
  int ntuple; // the number of tuple
};


/* information of field on table */
class FInfo {
 public:
  FType ftype;
  string fieldName;
  size_t offset;
  size_t length;
};


/* information of table schema */
class TSchema {
 public:
  void *head; // the pointer of 1st page.
  string tableName;
  size_t nField; // the number of field
  size_t pageSize;
  map<string, FInfo> fmap; 

  FInfo
  getFieldInfo(string str){
    return fmap[str];
  }
  void
  appendFieldInfo(FInfo finfo){
    fmap[finfo.fieldName] = finfo;
  }
  string
  getTableName(void){
    return tableName;
  }
  void
  setTableName(string str){
    tableName = str;
  }
};


/*
  MasterSchema has schema information of all table.
*/
class MasterSchema{
 private:

 public:
  static map<string, TSchema> tmap;

  static TSchema* getTableSchemaPtr(string str);
  static void appendTableSchema(TSchema& ts);
};

#endif
