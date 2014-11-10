#ifndef _MYMAP
#define _MYMAP

class DirtyPageTable;

typedef struct {
  uint32_t page_id;
  uint32_t rec_LSN;
} DP_entry;

class DirtyPageTableIterator : public std::iterator<std::forward_iterator_tag, DP_entry>{
  friend class DirtyPageTable;

private:
  DirtyPageTable* m_dp_table;
  int m_index;

  // 既定のコンストラクタでは、末端に相当するイテレータを作成する
  DirtyPageTableIterator();
  // このコンストラクタでは、扱うインスタンスと最初の位置情報を受け取ってイテレータを作成する
  DirtyPageTableIterator(DirtyPageTable* dp_table, int index);

public:
  DirtyPageTableIterator(const DirtyPageTableIterator& iterator);
  DirtyPageTableIterator& operator++();
  DirtyPageTableIterator operator++(int);
  bool operator!=(const DirtyPageTableIterator& iterator);
  bool operator==(const DirtyPageTableIterator& iterator);
  DP_entry& operator*();
  DP_entry* operator->();

};


class DirtyPageTable{
 friend class DirtyPageTableIterator;
private:
  DP_entry*table;
  int bucket_size;
  int item_size;

  std::mutex *locks;



 public:
  DirtyPageTable(int capacity);
  void add(uint32_t page_id, uint32_t rec_LSN);
  void remove(uint32_t page_id);
  bool contains(uint32_t page_id);
  DP_entry& operator[](int n);

  typedef DirtyPageTableIterator iterator;
  DirtyPageTable::iterator begin();
  DirtyPageTable::iterator end();

};


#endif 
