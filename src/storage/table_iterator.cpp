#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator() {}

TableIterator::TableIterator(Row row, TableHeap *source)
: row(row), source(source) {
}

TableIterator::TableIterator(const TableIterator &other)
: row(other.row), source(other.source) {}

TableIterator::~TableIterator() {
}

bool TableIterator::operator==(const TableIterator &itr) const {
    return row.GetRowId() == itr.row.GetRowId();
}

/* 代码复用 */
bool TableIterator::operator!=(const TableIterator &itr) const {
    return !((*this) == itr);
}

const Row &TableIterator::operator*() {
  return row;
}

Row *TableIterator::operator->() {
  return &row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  row = itr.row;
  source = itr.source;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  BufferPoolManager *bpm = source->buffer_pool_manager_;
  page_id_t currPgId = row.GetRowId().GetPageId();
  TablePage *currPg = reinterpret_cast<TablePage *>
                        (bpm->FetchPage(currPgId));
  RowId nextRID;
  Row *tmp = new Row(row.GetRowId());  // 临时变量
  bool isFound = false;

  ASSERT(currPg != nullptr, "Fetched null page in TableIt");

  /* 能够在当前页找到下一个tuple —— 直接访问并返回 */
  if(currPg->GetNextTupleRid(row.GetRowId(), &nextRID)){
    tmp->SetRowId(nextRID);         // 获取到了rid
    source->GetTuple(tmp, nullptr); // 获取到了tuple，此时row更新完毕
    row = *tmp;
    bpm->UnpinPage(currPgId, false);
    delete tmp;
    return (*this);
  }

  /* 不能够在当前页找到下一个tuple —— 往后找直至到末尾 */
  else {
    while(currPg->GetNextPageId() != INVALID_PAGE_ID) // 下一页非无效页
    {
      currPgId = currPg->GetPageId();
      bpm->UnpinPage(currPgId, false);
      currPg = reinterpret_cast<TablePage *>(bpm->FetchPage(currPg->GetNextPageId()));
      // 访问下一页
      if(currPg->GetFirstTupleRid(&nextRID)) {
        isFound = true;
        break;
      }
    }
  }

  if(isFound) {
    tmp->SetRowId(nextRID);         // 获得rid
    source->GetTuple(tmp, nullptr); // 获得tuple
    row = *tmp;
  }
  else  row = Row();

  bpm->UnpinPage(currPgId, false);
  delete tmp;

  return (*this);
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator history(*this);
  ++(*this);
  return history;
}
