//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"
#include "planner/expressions/abstract_expression.h"
#include "storage/table_heap.h"
#include "storage/table_iterator.h"

/**
* TODO: Student Implement
*/
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan){}

/* 找到对应表的首个元素 */
void SeqScanExecutor::Init() {
  /* 0. 获取sequential scan中的:
   ** 表名
   ** catalog */
  const std::string tableName = plan_->GetTableName();
  CatalogManager *catalog = GetExecutorContext()->GetCatalog();

  /* 1. 找到表头 */
  TableInfo *targetInfo;
  TableHeap *target;
  catalog->GetTable(tableName, targetInfo);
  target = targetInfo->GetTableHeap();
  tableIt = target->Begin(nullptr);
  end = target->End();
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  /* 0. 获取sequential scan中的筛选条件对应的expression tree和
   *    输入与输出输出Schema */
  AbstractExpressionRef filter = plan_->GetPredicate();
  const std::string tableName = plan_->GetTableName();
  // --------------------------------------------------------------
  CatalogManager *catalog = GetExecutorContext()->GetCatalog();
  TableInfo *targetInfo;
  catalog->GetTable(tableName, targetInfo);
  // --------------------------------------------------------------
  const Schema *schemaOut = GetOutputSchema(),
               *schemaIn = targetInfo->GetSchema();
  // 当有where的时候，predicate为非空指针，且应该一定是comparison或logic类型expression
  // 该expression返回类型一定为Field(kTypeInt, CmpBool::kTrue/kFalse)
  // 当没有where时，应该是空指针

  Field mark(kTypeInt, CmpBool::kTrue);

  /* !!!1. 对expression tree做遍历，找到查询语句的筛选条件
   * 以目前对框架的理解，expression tree会从根节点递归向下进行条件筛选
   * 当条件结果为CmpBool::kTrue时，说明该row符合条件 */
  while(tableIt != end)
  {
    Row *tmp = new Row(tableIt.operator*());
    /* 筛选条件存在 */
    if(filter)
    {
      if(filter->Evaluate(tmp).CompareEquals(mark))
      {
        *row = *tmp;
        *rid = tmp->GetRowId();
        row->GetKeyFromRow(schemaIn, schemaOut, *row);
        delete tmp;
        tableIt++;
        return true;
      }
      delete tmp;
      tableIt++;
    }
    /* 筛选条件不存在 */
    else
    {
      *row = *tmp;
      *rid = tmp->GetRowId();
      row->GetKeyFromRow(schemaIn, schemaOut, *row);
      delete tmp;
      tableIt++;
      return true;
    }
  }

  return false;
}
