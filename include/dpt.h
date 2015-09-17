#ifndef _MYMAP
#define _MYMAP

class DirtyPageTable;

typedef struct {
  uint32_t page_id;
  uint64_t rec_LSN;
  uint64_t rec_offset;
  uint32_t log_file_id;
} DP_Entry;

class DirtyPageTableIterator : public std::iterator<std::forward_iterator_tag, DP_Entry>{
  friend class DirtyPageTable;

private:
  DirtyPageTable* m_dp_table;
  int m_index;

  // このコンストラクタでは、扱うインスタンスと最初の位置情報を受け取ってイテレータを作成する
  DirtyPageTableIterator(DirtyPageTable* dp_table, int index);

public:
  // 既定のコンストラクタでは、末端に相当するイテレータを作成する
  DirtyPageTableIterator();
  DirtyPageTableIterator(const DirtyPageTableIterator& iterator);
  DirtyPageTableIterator& operator++();
  DirtyPageTableIterator operator++(int);
  bool operator!=(const DirtyPageTableIterator& iterator);
  bool operator==(const DirtyPageTableIterator& iterator);
  DP_Entry& operator*();
  DP_Entry* operator->();

};


class DirtyPageTable{
 friend class DirtyPageTableIterator;
private:
  DP_Entry*table;
  int bucket_size;
  int item_size;

  std::mutex *locks;



 public:
  DirtyPageTable(int capacity);
  void add(uint32_t page_id, uint64_t rec_LSN, uint64_t rec_offset, int log_file_id);
  void remove(uint32_t page_id);
  bool contains(uint32_t page_id);
  DP_Entry& operator[](int n);

  typedef DirtyPageTableIterator iterator;
  DirtyPageTable::iterator begin();
  DirtyPageTable::iterator end();

};


#endif 
