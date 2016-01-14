#ifndef _schema
#define _schema

#include <map>
#include <string>
#include <iostream>

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
  std::string fieldName;
  size_t offset;
  size_t length;
};


/* information of table schema */
class TableSchema {
 public:
  void *head; // the pointer of 1st page.
  std::string tableName;
  size_t nField; // the number of field
  size_t pageSize;
  std::map<std::string, FieldInfo> fmap;

  FieldInfo
  getFieldInfo(std::string str){
    return fmap[str];
  }
  void
  appendFieldInfo(FieldInfo finfo){
    fmap[finfo.fieldName] = finfo;
  }
  std::string
  getTableName(void){
    return tableName;
  }
  void
  setTableName(std::string str){
    tableName = str;
  }
};


/*
  MasterSchema has schema information of all table.
*/
class MasterSchema{
 private:

 public:
  static std::map<std::string, TableSchema> tmap;

  static TableSchema* getTableSchemaPtr(std::string str);
  static void appendTableSchema(TableSchema& ts);
};

#endif
