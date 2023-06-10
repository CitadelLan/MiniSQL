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

  /* 1. 获取values_executor中的元组 */
  if(child_executor_->Next(row, nullptr)) {
    /* ! 检测该row是否违背primary或unique属性 ! */
    Schema *schema = tableInfo->GetSchema();
    uint32_t col = 0;
//    Row indexKey = *row;
//    TableIterator begin = tableHeap->Begin(nullptr),
//                  end = tableHeap->End();
//
//    for(auto it = begin; it != end; it++) {
//      for (auto uni : tableInfo->GetMeta()->uni_keys) {
//        schema->GetColumnIndex(uni, col);
//        if (CmpBool::kTrue == it->GetField(col)->CompareEquals(*(row->GetField(col)))) {
//          cout << "Error: violated unique key " << uni << endl;
//          return false;
//        }
//      }
//      for (auto pri : tableInfo->GetMeta()->pri_keys) {
//        schema->GetColumnIndex(pri, col);
//        if (CmpBool::kTrue == it->GetField(col)->CompareEquals(*(row->GetField(col)))) {
//          cout << "Error: violated primary key " << pri << endl;
//          return false;
//        }
//      }
//    }
    /* 2. 插入元组 */
    if (tableHeap->InsertTuple(*row, nullptr)) {
      /* 2.1. 检查是否包含索引，如果有，更新索引 */
      const std::vector<Column *> columns = schema->GetColumns(0);
      std::vector<IndexInfo *> tmp;
      catalog->GetTableIndexes(tableName, tmp);
      IndexInfo *index;

      for(auto it : tmp)
      {
        if(catalog->GetIndex(tableName, it->GetIndexName(), index) == dberr_t::DB_SUCCESS) {
          for (auto it2 : columns)
          {
            schema->GetColumnIndex(it2->GetName(), col);
            std::vector<uint32_t> kMap = index->GetMeta()->GetKeyMapping();
            if(find(kMap.begin(), kMap.end(), col) != kMap.end()) {
              *rid = row->GetRowId();
              row->GetKeyFromRow(schema, index->GetIndexKeySchema(), *row);
              row->SetRowId(*rid);
              index->GetIndex()->InsertEntry(*row, row->GetRowId(), nullptr);
            }
          }
        }
      }

      return true;
    }
  }
  return false;
}