//
// Created by njz on 2023/1/27.
//
#include <algorithm>
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
  const Schema *schemaOut, *schemaIn = tableInfo->GetSchema();

  /* 1. 获取values_executor中的元组 */
  if(child_executor_->Next(row, nullptr)) {
    uint32_t col = 0;

    /* ! 检测该row是否违背primary或unique属性 ! */
    Row indexKey = *row;
    std::vector<IndexInfo *> indexes;

    catalog->GetTableIndexes(tableName, indexes);

    for(auto it : indexes)
    {
      std::vector<RowId> tmp;

      schemaOut = it->GetIndexKeySchema();
      indexKey.GetKeyFromRow(schemaIn, schemaOut, indexKey);
      it->GetIndex()->ScanKey(indexKey, tmp, nullptr);
      if(!tmp.empty())
      {
        cout << "Error: violated primary/unique attribute on table " << tableName << endl;
        return false;
      }
    }

    /* 2. 插入元组 */
    tableHeap->InsertTuple(*row, nullptr);
    *rid = row->GetRowId();
    for(auto index : indexes) {
        /* 2.1. 检查是否包含索引，如果有，更新索引 */
        Row tmp;
        row->GetKeyFromRow(schemaIn, index->GetIndexKeySchema(), tmp);
        index->GetIndex()->InsertEntry(tmp, *rid, nullptr);
    }

    return true;
  }

  return false;
}