#include "executor/executors/index_scan_executor.h"
#include <queue>
#include "planner/expressions/constant_value_expression.h"
#include <algorithm>

/* 对一个UpdateExecutor，有：
 ** ExecuteContext: CatalogManager + BufferPoolManager
 ** IndexScanPlanNode: output_schema + table_name + indexes(实际上只有单个索引) +
 *                     filter_predicate_
 * */

IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

bool cmp(RowId &a, RowId &b){
  return a.Get() == b.Get();
}

void IndexScanExecutor::Init() {
  ComparisonExpression indexComp = ComparisonExpression(nullptr, nullptr, "");

  /* 0. 遍历expression树准备，找到原schema */
  std::queue<AbstractExpression *> expTree;
  std::vector<ComparisonExpression *> compNodes;
  const std::string tableName = plan_->GetTableName();
  // --------------------------------------------------------------
  CatalogManager *catalog = GetExecutorContext()->GetCatalog();
  TableInfo *targetInfo;
  catalog->GetTable(tableName, targetInfo);
  // --------------------------------------------------------------
  const Schema *schemaOut = GetOutputSchema(),
               *schemaIn = targetInfo->GetSchema();

  expTree.push(plan_->GetPredicate().get());

  /* 1. 遍历expression树直到找到compare节点
   *    compare下面一个column，一个const，*/
  while(!expTree.empty())
  {
    AbstractExpression * currNode = expTree.front();

    if(currNode->GetType() == ExpressionType::ComparisonExpression)
      compNodes.push_back(reinterpret_cast<ComparisonExpression *>(currNode));

    /* 1.1. 如果下一层没到叶子层，push */
    for(auto it : currNode->GetChildren()) {
      if (it->GetType() == ExpressionType::LogicExpression ||
          it->GetType() == ExpressionType::ComparisonExpression)
        expTree.push(it.get());
    }

    expTree.pop();
  }

  /* 2. 得到comparison节点，判断是不是index里的节点 */
  std::vector<RowId> results;
  Row indexKey;
  IndexInfo *indexInfo;
  for(auto it : compNodes) {
    int isFound = 0;
    for (const auto &childIt : it->GetChildren()) {
      /* 2.1. 找到了ColumnExpression节点，检验其对应下标是否与index中的一致 */
      if (childIt->GetType() == ExpressionType::ColumnExpression) {
        auto *index = reinterpret_cast<ColumnValueExpression *>(childIt.get());
        for (int i = 0; i < plan_->indexes_.size(); i++) {
          indexInfo = plan_->indexes_[i];
          std::vector<uint32_t> kMap = indexInfo->GetMeta()->GetKeyMapping();
          if (find(kMap.begin(), kMap.end(), index->GetColIdx()) != kMap.end()) {
            indexComp = *reinterpret_cast<ComparisonExpression *>(it);
            isFound++;
          }
        }
      } else {
        auto *indexRely = reinterpret_cast<ConstantValueExpression *>(childIt.get());
        Field indexField(indexRely->val_);
        std::vector<Field> tmp;
        tmp.push_back(indexField);
        indexKey = Row(tmp);
        // indexKey.GetKeyFromRow(schemaIn, schemaOut, indexKey);
        isFound++;
      }

      /* 2.2. 得到符合index条件的节点，判断ComparisonExpression的比较类型，并通过其来寻找符合index条件的集合 */
      if (isFound == 2) {
        std::string comparator = indexComp.GetComparisonType();
        auto *bpIndex = reinterpret_cast<BPlusTreeIndex *>(indexInfo->GetIndex());
        if(it == compNodes[0]) {
          bpIndex->ScanKey(indexKey, results, nullptr, comparator);
          list = results;
        }
        else {
          std::vector<RowId> tmp;
          bpIndex->ScanKey(indexKey, tmp, nullptr, comparator);
          set_intersection(results.begin(), results.end(), tmp.begin(), tmp.end(), list.begin(), cmp);
        }
        break;
      }

    }

  }
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  /* 0. 获取index scan中的筛选条件对应的expression tree、TableHeap和
   *    输入与输出输出Schema */
  AbstractExpressionRef filter = plan_->GetPredicate();
  const std::string tableName = plan_->GetTableName();
  // --------------------------------------------------------------
  CatalogManager *catalog = GetExecutorContext()->GetCatalog();
  TableInfo *targetInfo;
  catalog->GetTable(tableName, targetInfo);
  // --------------------------------------------------------------
  TableHeap *tableHeap = targetInfo->GetTableHeap();
  const Schema *schemaOut = GetOutputSchema(),
               *schemaIn = targetInfo->GetSchema();
  // 当有where的时候，predicate为非空指针，且应该一定是comparison或logic类型expression
  // 该expression返回类型一定为Field(kTypeInt, CmpBool::kTrue/kFalse)
  // 当没有where时，应该是空指针

  Field mark(kTypeInt, CmpBool::kTrue);
  auto it = list.begin();
  int i = 0;

  if(!list.empty())
  {
    Row *tmp = new Row(list[0]);
    tableHeap->GetTuple(tmp, nullptr);
    /* 筛选条件存在 */
    if(filter) {
      while (!filter->Evaluate(tmp).CompareEquals(mark)) {
        delete tmp;
        list.erase(it);
        i++;
        it++;
        if(i >= list.size())  return false;
        tmp = new Row(list[i]);
        tableHeap->GetTuple(tmp, nullptr);
      }
    }

    tableHeap->GetTuple(tmp, nullptr);
    *row = *tmp;
    *rid = tmp->GetRowId();
    row->GetKeyFromRow(schemaIn, schemaOut, *row);
    // 输出需要按照OutputSchema格式
    delete tmp;
    list.erase(it);

    return true;
  }

  return false;
}