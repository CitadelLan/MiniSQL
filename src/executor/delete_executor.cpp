//
// Created by njz on 2023/1/29.
//

#include <algorithm>
#include "executor/executors/delete_executor.h"

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/* 对一个UpdateExecutor，有：
 ** ExecuteContext: CatalogManager + BufferPoolManager
 ** UpdatePlanNode: (output_schema) + table_name +
 *                  children(单个SeqScanPlanNode) +
 *                  update_attrs (uint32/field下标 -> AbstractExpression/新field)
 *  child_executor: children->SeqScanExecutor
 * */

void DeleteExecutor::Init() {
  child_executor_->Init();
}

bool DeleteExecutor::Next(Row *row, RowId *rid) {
  /* 0. 获取相关表 */
  CatalogManager *catalog = exec_ctx_->GetCatalog();
  std::string tableName = plan_->GetTableName();
  TableInfo *tableInfo;
  TableHeap *tableHeap;
  catalog->GetTable(tableName, tableInfo);
  tableHeap = tableInfo->GetTableHeap();

  /* 1. 获取SeqScanExecutor中的元组
   * 按照SeqScanExecutor的逻辑，row和rid本身地址来自ExecutePlan，值来自SeqScanExecutor */
  if(child_executor_->Next(row, rid))
  {
    /* 2. 删除 */
    /* 2.1. 标记待删除元组 */
    if(tableHeap->MarkDelete(*rid, nullptr)) {
      /* 2.2. 检查是否包含索引，如果有，删除索引 */
      Schema *schema = tableInfo->GetSchema();
      const std::vector<Column *> columns = schema->GetColumns(0);
      std::vector<IndexInfo *> tmp;
      catalog->GetTableIndexes(tableName, tmp);
      IndexInfo *index;

      for(auto it : tmp)
      {
        if(catalog->GetIndex(tableName, it->GetIndexName(), index) == dberr_t::DB_SUCCESS) {
          for (auto it2 : columns)
          {
            uint32_t col;
            schema->GetColumnIndex(it2->GetName(), col);
            std::vector<uint32_t> kMap = index->GetMeta()->GetKeyMapping();
            if(find(kMap.begin(), kMap.end(), col) != kMap.end()) {
              Row tmp;
              row->GetKeyFromRow(schema, index->GetIndexKeySchema(), tmp);
              index->GetIndex()->RemoveEntry(tmp, *rid, nullptr);
            }
          }
        }
      }

      tableHeap->ApplyDelete(*rid, nullptr);

      return true;
    }
  }

  return false;
}