//
// Created by njz on 2023/1/30.
//

#include <algorithm>
#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/* 对一个UpdateExecutor，有：
 ** ExecuteContext: CatalogManager + BufferPoolManager
 ** UpdatePlanNode: (output_schema) + table_name +
 *                  children(单个SeqScanPlanNode) +
 *                  update_attrs (uint32/field下标 -> AbstractExpression/新field)
 *  child_executor: children->SeqScanExecutor
 * */

void UpdateExecutor::Init() {
  child_executor_->Init();
}

bool UpdateExecutor::Next(Row *row, RowId *rid) {
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
    /* 2. 更新 */
    /* 2.2. 检查是否包含索引，如果有，更新索引 */
    Schema *schema = tableInfo->GetSchema();
    const std::vector<Column *> columns = schema->GetColumns(0);
    std::vector<IndexInfo *> tmp;
    catalog->GetTableIndexes(tableName, tmp);
    IndexInfo *index;
    Row newTuple = GenerateUpdatedTuple(*row); // 获取更新的新元组

    for(auto it : tmp)
    {
      if(catalog->GetIndex(tableName, it->GetIndexName(), index) == dberr_t::DB_SUCCESS) {
        for (auto it2 : columns)
        {
          uint32_t col;
          schema->GetColumnIndex(it2->GetName(), col);
          std::vector<uint32_t> kMap = index->GetMeta()->GetKeyMapping();
          if(find(kMap.begin(), kMap.end(), col) != kMap.end()) {
            Row tmpRow, removal;
            std::vector<RowId> result;
            newTuple.GetKeyFromRow(schema, index->GetIndexKeySchema(), tmpRow);
            row->GetKeyFromRow(schema, index->GetIndexKeySchema(), removal);
            index->GetIndex()->ScanKey(tmpRow, result, nullptr);
            if(result.empty())
            {
              tableHeap->MarkDelete(*rid, nullptr);
              tableHeap->ApplyDelete(*rid, nullptr);
              tableHeap->InsertTuple(newTuple, nullptr);
              index->GetIndex()->InsertEntry(tmpRow, newTuple.GetRowId(), nullptr);
              index->GetIndex()->RemoveEntry(removal, *rid, nullptr);

              return true;
            }
            else
            {
              cout << "Error: updated tuples violated primary/unique key attribute." << endl;
              return false;
            }
          }
        }
      }
    }

    tableHeap->MarkDelete(*rid, nullptr);
    tableHeap->ApplyDelete(*rid, nullptr);
    tableHeap->InsertTuple(newTuple, nullptr);

    return true;
  }

  return false;
}

Row UpdateExecutor::GenerateUpdatedTuple(Row src_row) {
  std::vector<Field *> srcField = src_row.GetFields();
  std::vector<Field> retField;
  std::unordered_map<uint32_t, AbstractExpressionRef> update_attrs;
  update_attrs = plan_->GetUpdateAttr();

  for(uint32_t i = 0; i < src_row.GetFieldCount(); i++) {
    /* 可以找到对应要更新的数值 */
    if (update_attrs.find(i) != plan_->update_attrs_.end())
      retField.push_back(update_attrs[i]->Evaluate(nullptr));
    else
      retField.push_back(*srcField[i]);
  }

  return Row(retField);
}