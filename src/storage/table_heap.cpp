#include "storage/table_heap.h"

/* Transaction 非必须 */

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  /* 找到table heap中第一个页，unpin该页释放缓存区（recent used） */
  TablePage *currPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  TablePage *targetPage = nullptr;

  buffer_pool_manager_->UnpinPage(first_page_id_, false);

  /* 插入失败，根据first fit原则，去到下一个页
   * 如果没有通过INVALID_PAGE_ID找到目标页，但同样插入成功，这一情况同样可以接受 */
  while (!currPage->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
    page_id_t nextPgId = currPage->GetNextPageId();  // 下个页的页id
    /* 下一页为无效页，即找到了目标页，分配空间 */
    if (nextPgId == INVALID_PAGE_ID) {
      targetPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(nextPgId));
      // nextPgId作为引用，会自动修改成分配好空间的PageId

      /* 分配空间失败 */
      if (nextPgId == INVALID_PAGE_ID) return false;

      /* 对新的page做双向链接 */
      targetPage->Init(nextPgId, currPage->GetPageId(), log_manager_, txn);
      currPage->SetNextPageId(targetPage->GetPageId());

      /* 对于新分配空间的页，需要填充数据 */
      targetPage->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    }
    currPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(nextPgId));
    buffer_pool_manager_->UnpinPage(currPage->GetPageId(), false);
  }

  /* 插入成功（新增页都是脏页）*/
  buffer_pool_manager_->UnpinPage(currPage->GetPageId(), true);

  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  TablePage *currPg = reinterpret_cast<TablePage *>
      (buffer_pool_manager_->FetchPage(rid.GetPageId()));
  bool update_indicator;

  if(currPg)  return false; // 未找到页

  /* 保存当前Page */
  Row historyRow(rid);

  /* 更新页，由于其未存储至硬盘，标记为脏页 */
  update_indicator =currPg->UpdateTuple(row, &historyRow, schema_, txn,
                          lock_manager_, log_manager_);
  buffer_pool_manager_->UnpinPage(currPg->GetPageId(), true);

  return update_indicator;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  TablePage *currPg = reinterpret_cast<TablePage *>
      (buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(currPg)  return; // 未找到页
  // Step2: Delete the tuple from the page.
  currPg->ApplyDelete(rid, txn, log_manager_);
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  /* 获得当前页 */
  page_id_t currPgId = row->GetRowId().GetPageId();
  TablePage *currPg = reinterpret_cast<TablePage *>
      (buffer_pool_manager_->FetchPage(currPgId));
  bool get_indicator;

  /* 页未获取——异常处理 */
  if(!currPg)
    return false;
  else {
   get_indicator =
        currPg->GetTuple(row, schema_, txn, lock_manager_);
    buffer_pool_manager_->UnpinPage(currPgId, false);
    return get_indicator;
  }
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction *txn) {
  TablePage *currPg;
  page_id_t currPgId = first_page_id_;
  RowId headRID = INVALID_ROWID;
  Row  *retRow = nullptr;
  bool isFound;

  /* Begin最大的问题在于原来TableHeap里存储的首页id可能被删除
   * 所以在Begin中需要重新定位实际的首页id，并做重定向 */
  while(currPgId != INVALID_PAGE_ID)
  {
    currPg = reinterpret_cast<TablePage *>
        (buffer_pool_manager_->FetchPage(currPgId));
    isFound = currPg->GetFirstTupleRid(&headRID);
    buffer_pool_manager_->UnpinPage(currPgId, false);
    if(isFound) break;
    currPgId = currPg->GetNextPageId();
  }

  if(isFound){
    retRow = new Row(headRID);
    GetTuple(retRow, nullptr);
    return TableIterator(retRow, this);
  }

  return End();
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  return TableIterator(nullptr, this);
}
