//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/* 对一个InsertExecutor，有：
 ** ExecuteContext: CatalogManager + BufferPoolManager
 ** InsertPlanNode: (output_schema) + table_name
 ** child_executor: values_executor: 其next方法返回下一个用于插入的元组
 * */

void InsertExecutor::Init() {
  child_executor_->Init();
}

/* 可能需要validate得到的value类型（待定） */
bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  /* 0. 获取相关表 */
  CatalogManager *catalog = exec_ctx_->GetCatalog();
  std::string tableName = plan_->GetTableName();
  TableInfo *tableInfo;
  TableHeap *tableHeap;
  catalog->GetTable(tableName, tableInfo);
  tableHeap = tableInfo->GetTableHeap();

  /* 1. 获取values_executor中的元组 */
  child_executor_->Next(row, nullptr);

  /* 2. 插入元组 */
  if(tableHeap->InsertTuple(*row, nullptr)) {
    /* 2.1. 检查是否包含索引，如果有，更新索引 */
    Schema *schema = tableInfo->GetSchema();
    const std::vector<Column *> columns = schema->GetColumns(0);
    IndexInfo *index = nullptr;

    for(auto it : columns)
      if(catalog->GetIndex(tableName, it->GetName(), index) == dberr_t::DB_SUCCESS)
        index->GetIndex()->InsertEntry(*row, *rid, nullptr);

    return true;
  }

  return false;
}