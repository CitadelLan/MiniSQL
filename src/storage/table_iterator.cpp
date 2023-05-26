#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator() {}

TableIterator::TableIterator(Row *row, TableHeap *source)
: row(row), source(source) {}

TableIterator::TableIterator(const TableIterator &other)
: row(other.row), source(other.source) {}

TableIterator::~TableIterator() {}

bool TableIterator::operator==(const TableIterator &itr) const {
  return row->GetRowId() == itr.row->GetRowId();
}

/* 代码复用 */
bool TableIterator::operator!=(const TableIterator &itr) const {
  return !((*this) == itr);
}

const Row &TableIterator::operator*() {
  ASSERT(row != nullptr, "Nullptr visited in TableIt.");
  return (*row);
}

Row *TableIterator::operator->() {
  ASSERT(row != nullptr, "Nullptr visited in TableIt.");
  return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  row = itr.row;
  source = itr.source;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  BufferPoolManager *bpm = source->buffer_pool_manager_;
  page_id_t currPgId = row->GetRowId().GetPageId();
  TablePage *currPg = reinterpret_cast<TablePage *>
                        (bpm->FetchPage(currPgId));
  RowId nextRID;
  bool isFound = false;

  ASSERT(currPg != nullptr, "Fetched null page in TableIt");

  /* 能够在当前页找到下一个tuple —— 直接访问并返回 */
  if(currPg->GetNextTupleRid(row->GetRowId(), &nextRID)){
    row->SetRowId(nextRID);         // 获取到了rid
    source->GetTuple(row, nullptr); // 获取到了tuple，此时row更新完毕
    bpm->UnpinPage(currPgId, false);
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

  delete row; // 释放空间防止内存泄漏
  if(isFound) {
    row = new Row(nextRID);         // 获得rid
    source->GetTuple(row, nullptr); // 获得tuple
  }
  else  row = nullptr;

  bpm->UnpinPage(currPgId, false);

  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator history(*this);
  ++(*this);
  return history;
}
