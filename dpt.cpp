#include <list>
#include <mutex>
#include <iterator>
#include <cstddef>
#include "ARIES.h"
#include "dpt.h"

// 既定のコンストラクタでは、末端に相当するイテレータを作成する
DirtyPageTableIterator::DirtyPageTableIterator(){
  m_dp_table = (DirtyPageTable *)NULL;
  m_index = -1;
}

// このコンストラクタでは、扱うインスタンスと最初の位置情報を受け取ってイテレータを作成する
DirtyPageTableIterator::DirtyPageTableIterator(DirtyPageTable* dp_table, int index){
  m_dp_table = dp_table;
  m_index = (index == 0 ? index : -1);

  while(!(*m_dp_table).contains(m_index)){
    m_index++;
  }
}
  
DirtyPageTableIterator::DirtyPageTableIterator(const DirtyPageTableIterator& iterator){
  m_dp_table = iterator.m_dp_table;
  m_index = iterator.m_index;
}

DP_Entry&
DirtyPageTableIterator::operator*(){
  // 末端だった時に返すダミー変数
  static DP_Entry dummy;

  return (m_index != -1 ? (*m_dp_table)[m_index] : dummy);
}

DP_Entry*
DirtyPageTableIterator::operator->(){

  return &(*m_dp_table)[m_index];
}

DirtyPageTableIterator&
DirtyPageTableIterator::operator++(){
  if(m_index == -1)
    return *this;

  m_index++;
  while(m_index < m_dp_table->bucket_size && !(*m_dp_table).contains(m_index)){
    if(m_index >= m_dp_table->bucket_size-1){ m_index = -1; m_dp_table=(DirtyPageTable *)NULL; break; }
    m_index++;
  }

  return *this;
}

DirtyPageTableIterator
DirtyPageTableIterator::operator++(int){
  if(m_index == -1)
    return *this;

  DirtyPageTableIterator result = *this;

  m_index++;
  while(m_index < m_dp_table->bucket_size && !(*m_dp_table).contains(m_index)){
    if(m_index >= m_dp_table->bucket_size-1){ m_index = -1; m_dp_table=(DirtyPageTable *)NULL; break; }
    m_index++;
  }

  return result;
}

bool
DirtyPageTableIterator::operator!=(const DirtyPageTableIterator& iterator){
  return this->m_dp_table != iterator.m_dp_table || this->m_index != iterator.m_index;
}

bool
DirtyPageTableIterator::operator==(const DirtyPageTableIterator& iterator){
  return !(*this != iterator);
}



DirtyPageTable::DirtyPageTable(int capacity) {
  bucket_size = capacity;
  item_size = 0;
    
  table = new DP_Entry[capacity];
  locks = new std::mutex[capacity];
}

void
DirtyPageTable::add(uint32_t page_id, uint32_t rec_LSN){
  if(contains(page_id)) return;
  //  printf("called add(%d,%d)\n", page_id,rec_LSN);

  int my_bucket = page_id % bucket_size; // 現在の実装ではバケット数はページ数に等しいためpage_id == my_bucketとなる.
  std::lock_guard<std::mutex> lock(locks[my_bucket]);
  if(table[my_bucket].rec_LSN == 0){
    table[my_bucket].page_id = page_id;
    table[my_bucket].rec_LSN = rec_LSN;
    item_size++;
  }
}

void
DirtyPageTable::remove(uint32_t page_id){
  int my_bucket = page_id % bucket_size;
  std::lock_guard<std::mutex> lock(locks[my_bucket]);
  if(table[my_bucket].rec_LSN != 0){
    table[my_bucket].rec_LSN = 0;
    item_size--;
  }
}

bool
DirtyPageTable::contains(uint32_t page_id){
  int my_bucket = page_id % bucket_size;
  std::lock_guard<std::mutex> lock(locks[my_bucket]);
  return table[my_bucket].rec_LSN!=0 ? true : false;
}

DP_Entry&
DirtyPageTable::operator[](int n){
  if(n < 0 || n >= (int)bucket_size){
    printf("index is out of range\n"); exit(1);
  }
  return table[n];
}  


// begin 関数では、最初の位置を示すイテレータを作って返します.
DirtyPageTable::iterator
DirtyPageTable::begin(){
  if(item_size==0) return DirtyPageTableIterator();
  return DirtyPageTableIterator(this,0);
}

// end 関数では、末端位置（最後の要素の次の位置）を示すイテレータを返します.
DirtyPageTable::iterator
DirtyPageTable::end(){
  return DirtyPageTableIterator();
}
