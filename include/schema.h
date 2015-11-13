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

enum FieldType {TYPE_INT, TYPE_DOUBLE, TYPE_CHAR, TYPE_NONE};

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
class FieldInfo {
 public:
  FieldType ftype;
  string fieldName;
  size_t offset;
  size_t length;
};


/* information of table schema */
class TableSchema {
 public:
  void *head; // the pointer of 1st page.
  string tableName;
  size_t nField; // the number of field
  size_t pageSize;
  map<string, FieldInfo> fmap;

  FieldInfo
  getFieldInfo(string str){
    return fmap[str];
  }
  void
  appendFieldInfo(FieldInfo finfo){
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
  static map<string, TableSchema> tmap;

  static TableSchema* getTableSchemaPtr(string str);
  static void appendTableSchema(TSchema& ts);
};

#endif
