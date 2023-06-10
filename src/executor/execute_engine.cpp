#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

#define ENABLE__EXECUTE_DEBUG

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code. **/
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
  // end of comment
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns(0)) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns(0)) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  if(ast == nullptr || ast->child_ == nullptr)
    return DB_FAILED;

  /* 1. database存在，返回报错 */
  std::string db_name(ast->child_->val_);
  if(dbs_.find(db_name) != dbs_.end())
  {
    std::cout << "Error: Database " << db_name << " exists." << endl;
    return DB_FAILED;
  }

  /* 2. database不存在，新建其 */
  auto* new_database = new DBStorageEngine(db_name, true);
  dbs_[db_name] = new_database;
  std::cout << "Database " << db_name << " created." << endl;
  // this->SaveDBs();

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  if(ast == nullptr || ast->child_ == nullptr)
    return DB_FAILED;

  /* 1. database不存在，返回报错 */
  std::string del_db_name(ast->child_->val_);
  if(dbs_.find(del_db_name) == dbs_.end()) {
    std::cout << "Error: Database " << del_db_name << " not found." << endl;
    return DB_FAILED;
  }

  /* 2. database存在，删除该数据库（如果当前数据库恰好为将被删除的库，转移位置） */
  if(current_db_ == del_db_name)
    current_db_ = "";
  DBStorageEngine* target_db = dbs_[del_db_name];
  target_db->~DBStorageEngine();
  dbs_.erase(del_db_name);
  std::cout << "Database " << del_db_name << " deleted." << endl;
  // this->SaveDBs();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  std::cout << "Number of database(s): " << dbs_.size() << endl;

  for(std::pair<std::string, DBStorageEngine*> it : dbs_)
    std::cout << it.first << std::endl;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  if(ast == nullptr || ast->child_ == nullptr)
    return DB_FAILED;

  /* 找不到database */
  std::string db_name(ast->child_->val_);
  if(dbs_.find(db_name) == dbs_.end())
  {
    std::cout << "No database called " << db_name << endl;
    return DB_FAILED;
  }

  /* 找到了database */
  current_db_ = db_name;
  std::cout << "Database " << db_name << " in use now." << endl;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  DBStorageEngine* current_db_engine = dbs_[current_db_];

  /* 1. 判断当前是否有已选中的dbEngine */
  if(current_db_engine == nullptr)
  {
    std::cout << "No database selected." << endl;
    return DB_FAILED;
  }

  /* 2. 获取该dbEngine中所有的table */
  std::vector<TableInfo *> tableInfos;
  current_db_engine->catalog_mgr_->GetTables(tableInfos);
  for(auto it : tableInfos)
    cout << it->GetTableName() << endl;
  cout << "Number of table(s): " << tableInfos.size() << endl;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  DBStorageEngine* current_db_engine = dbs_[current_db_];

  if(current_db_engine == nullptr) {
    cout << "Error on fetching database: " << current_db_ << " ." << endl;
    return DB_FAILED;
  }

  /* 1. 查找表在该database中是否存在 */
  string new_table_name(ast->child_->val_);
  TableInfo* new_table_info;
  dberr_t find_table = current_db_engine->catalog_mgr_
                           ->GetTable(new_table_name, new_table_info);
  if(find_table == DB_SUCCESS)
  {
    std::cout << "Table exists in current database." << endl;
    return DB_FAILED;
  }

  TableInfo* tmp_table_info;                      // 存放新表信息
  /* 语法树 */
  pSyntaxNode currList = ast->child_->next_;
  pSyntaxNode currNode = currList->child_;
  /* Columns */
  vector<Column*> tmp_column_vec;                 // 新表栏目（schema）
  vector<string> column_names;                    // 新表个栏名字——便于后续调用
  /* Columns属性 */
  unordered_map<string, bool> is_unique_key;
  unordered_map<string, bool> is_primary_key;
  unordered_map<string, string> type_of_column;
  unordered_map<string, int> char_size;
  vector<string> uni_keys;
  vector<string> pri_keys;

  /* 2. 解析Column信息 */
  while(currNode != nullptr && currNode->type_ == kNodeColumnDefinition)
  {
    /* 2.0. 默认值设定 */
    string currTypename(currNode->child_->next_->val_);
    string currColumnName(currNode->child_->val_);
    column_names.push_back(currColumnName);
    type_of_column[currColumnName] = currTypename;
    is_unique_key[currColumnName] = false;
    is_primary_key[currColumnName] = false;

    /* 2.1. 判断是否是unique属性 */
    bool isUnique = false;
    if(currNode->val_ != nullptr)
      isUnique = true;
    if(isUnique)
    {
      is_unique_key[currColumnName] = true;
      uni_keys.push_back(currColumnName);
    }
    else
      is_unique_key[currColumnName] = false;

    /* 2.2. 对于char属性特判 */
    if(currTypename == "char")
    {
      pSyntaxNode charNode = currNode->child_->next_->child_;
      string str_char_size (charNode->val_);
      char_size[currColumnName] = atoi(str_char_size.data());

      if(char_size[currColumnName] <= 0)
      {
        std::cout << "Error: char " << currColumnName << " size <= 0 !" << endl;
        return DB_FAILED;
      }
    }

    currNode = currNode->next_;
  }

  /* 3. 主键定义
   * currNode->type_ != kNodeColumnDefinition */
  if(currNode != nullptr)
  {
    pSyntaxNode primary_keys_node = currNode->child_;
    while(primary_keys_node)
    {
      string primary_key_name(primary_keys_node->val_);
      is_primary_key[primary_key_name] = true;
      pri_keys.push_back(primary_key_name);
      // uni_keys.push_back(primary_key_name);
      primary_keys_node = primary_keys_node->next_;
    }

  }

  /* 4. 对收集到的columns做实例化 */
  int column_index_counter = 0;
  for(const string& column_name_stp: column_names)
  {
    Column* new_column;

    /* int类型 */
    if(type_of_column[column_name_stp] == "int")
    {
      if(is_unique_key[column_name_stp] || is_primary_key[column_name_stp])
        new_column = new Column(column_name_stp, TypeId::kTypeInt, column_index_counter,
                                false, true);
      else
        new_column = new Column(column_name_stp, TypeId::kTypeInt, column_index_counter,
                                false, false);
    }

    /* float类型 */
    else if(type_of_column[column_name_stp] == "float")
    {
      if(is_unique_key[column_name_stp] || is_primary_key[column_name_stp])
        new_column = new Column(column_name_stp, TypeId::kTypeFloat, column_index_counter,
                                false, true);
      else
        new_column = new Column(column_name_stp, TypeId::kTypeFloat, column_index_counter,
                                false, false);
    }

    /* char类型 */
    else if(type_of_column[column_name_stp] == "char") {
      if (is_unique_key[column_name_stp] || is_primary_key[column_name_stp])
        new_column = new Column(column_name_stp, TypeId::kTypeChar, char_size[column_name_stp], column_index_counter,
                                false, true);
      else
        new_column = new Column(column_name_stp, TypeId::kTypeChar, char_size[column_name_stp], column_index_counter,
                                false, false);
    }
    else
    {
      std::cout << "Error: Unknown typename" << column_name_stp << endl;
      return DB_FAILED;
    }
    column_index_counter++;
    tmp_column_vec.push_back(new_column);
  }

  /* 5. 将实例化的columns信息构造为TableInfo类型对象
   *    ? 需要进一步区分主键与unique键 */
  auto* new_schema = new Schema(tmp_column_vec);
  dberr_t if_create_success;
  if_create_success = current_db_engine->catalog_mgr_->CreateTable(new_table_name,
                                                                   new_schema, nullptr, tmp_table_info);
  if(if_create_success != DB_SUCCESS)
    return if_create_success;
  CatalogManager* current_CMgr = dbs_[current_db_]->catalog_mgr_;
  for(const string& column_name_stp: column_names)
  {
    /* 这里不清楚到底是用unique还是primary */
    if(is_primary_key[column_name_stp])
    {
      string stp_index_name = column_name_stp + "_index";
      vector<string> index_columns_stp = {column_name_stp};
      IndexInfo* stp_index_info;
      dberr_t if_create_index_success
          = current_CMgr->CreateIndex(new_table_name, stp_index_name, index_columns_stp,
                                      nullptr, stp_index_info, "bptree");
      if(if_create_index_success != DB_SUCCESS)
        return if_create_index_success;
    }
  }

  tmp_table_info->GetMeta()->pri_keys = pri_keys;
  tmp_table_info->GetMeta()->uni_keys = uni_keys;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if(ast == nullptr || ast->child_ == nullptr)
    return DB_FAILED;

  string drop_table_name(ast->child_->val_);

  return dbs_[current_db_]->catalog_mgr_->DropTable(drop_table_name);
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  /* 1. 查找表是否存在 */
  vector<TableInfo*> vec_tableInfo;
  dberr_t if_gettable_success = dbs_[current_db_]->catalog_mgr_->GetTables(vec_tableInfo);
  if(if_gettable_success != DB_SUCCESS)
    return if_gettable_success;

  /* 2. 查找index */
  CatalogManager* current_CMgr = dbs_[current_db_]->catalog_mgr_;
  for(TableInfo* tmp_tableInfo: vec_tableInfo)
  {
    vector<IndexInfo*> vec_tmp_indexInfo;
    string table_name = tmp_tableInfo->GetTableName();
    dberr_t if_getIndex_success = current_CMgr->GetTableIndexes(table_name, vec_tmp_indexInfo);
    if(if_getIndex_success != DB_SUCCESS)
      return if_getIndex_success;
    for(auto it : vec_tmp_indexInfo)
    {
      string index_name = it->GetIndexName();
      std::cout << index_name << endl;
    }
  }

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(ast == nullptr || current_db_ == "")
    return DB_FAILED;

  /* 0. 查找表 */
  CatalogManager* current_CMgr = dbs_[current_db_]->catalog_mgr_;
  string table_name(ast->child_->next_->val_);
  string index_name(ast->child_->val_);
  TableInfo* target_table;
  dberr_t if_gettable_success = current_CMgr->GetTable(table_name, target_table);
  if(if_gettable_success != DB_SUCCESS)
    return if_gettable_success;

  /* 1. 查找需要建立索引的项目 */
  vector<string> vec_index_colum_lists;
  pSyntaxNode pSnode_colum_list = ast->child_->next_->next_->child_;
  while(pSnode_colum_list)
  {
    vec_index_colum_lists.emplace_back(pSnode_colum_list->val_);
    pSnode_colum_list = pSnode_colum_list->next_;
  }

  /* 2. 寻找索引是否存在 */
  Schema* target_schema = target_table->GetSchema();
  for(const string& tmp_colum_name: vec_index_colum_lists)
  {
    bool if_could = false;
    vector<string> uni_colum_list = target_table->GetMeta()->uni_keys;

    /* 2.1. 检验该columnName是否可以建立索引项 */
    for(const string& name: uni_colum_list)
    {
      if(name == tmp_colum_name)
      {
        if_could = true;
        break;
      }
    }
    if(!if_could)
    {
      std::cout << "Error: Can't build index on column(s) not unique." << endl;
      return DB_FAILED;
    }

    /* 2.2. 获取索引项下标 */
    uint32_t tmp_index;
    dberr_t if_getColumn_success = target_schema->GetColumnIndex(tmp_colum_name, tmp_index);
    if(if_getColumn_success != DB_SUCCESS)
      return if_getColumn_success;
  }

  /* 3. 建立索引 */
  IndexInfo* new_indexInfo;
  dberr_t if_createIndex_success = current_CMgr->CreateIndex
                                   (table_name, index_name, vec_index_colum_lists, nullptr,
                                    new_indexInfo, "bptree");
  if(if_createIndex_success != DB_SUCCESS)
    return if_createIndex_success;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if(ast == nullptr || current_db_.empty())
    return DB_FAILED;

  CatalogManager* current_CMgr = dbs_[current_db_]->catalog_mgr_;
  string index_name(ast->child_->val_);
  vector<TableInfo*> table_names;
  current_CMgr->GetTables(table_names);
  string table_name;
  bool isFound = false;

  /* 1. 查找该索引 */
  for(TableInfo* tmp_info: table_names)
  {
    vector<IndexInfo*> index_infos;
    current_CMgr->GetTableIndexes(tmp_info->GetTableName(), index_infos);
    for(IndexInfo* stp_idx_info: index_infos)
    {
      if(stp_idx_info->GetIndexName() == index_name)
      {
        table_name = tmp_info->GetTableName();
        isFound = true;
        break;
      }
    }
    if(isFound) break;
  }

  /* 2. 删除索引 */
  IndexInfo* tmp_indexInfo;
  dberr_t if_getIndex_success = current_CMgr->GetIndex(table_name, index_name, tmp_indexInfo);

  if(if_getIndex_success != DB_SUCCESS)
  {
    std::cout << "Error: No index: " << index_name << endl;
    return if_getIndex_success;
  }

  dberr_t if_create_success = current_CMgr->DropIndex(table_name, index_name);
  if(if_create_success != DB_SUCCESS)
  {
    std::cout << "Error: Fail to drop index: " << index_name << endl;
    return if_create_success;
  }

  return DB_SUCCESS;
}

//可不实现
dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

//可不实现
dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

//可不实现
dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  if(ast == nullptr || current_db_.empty())
    return DB_FAILED;

  string filename(ast->child_->val_);
  fstream exeFile(filename);
  if(!exeFile.is_open())
  {
    std::cout << "Error: Fail to open '" << filename << "'." << endl;
    return DB_FAILED;
  }

  int buffer_size = 1024;
  char* cmd = new char[buffer_size];

  while (1)
  {
    char tmp_char;
    int tmp_counter = 0;

    /* 1. 读入文件 */
    do
    {
      if(exeFile.eof())
      {
        // LOG(INFO) << "Before delete: " << cmd << endl;
        delete []cmd;
        break;
      }
      tmp_char = exeFile.get();
      cmd[tmp_counter++] = tmp_char;
      if(tmp_counter >= buffer_size)
      {
        std::cout << "buffer overflow" << endl;
        return DB_FAILED;
      }
    } while(tmp_char != ';');

    /* 2. 对文件输入的指令进行语法树分析 */
    // LOG(INFO) << "Command: " << cmd << endl;
    cmd[tmp_counter] = 0;
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr)
    {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);
    MinisqlParserInit();
    yyparse();
    if (MinisqlParserGetError())
    {
      printf("%s\n", MinisqlParserGetErrorMessage());
      return DB_FAILED;
    }
    this->Execute(MinisqlGetParserRootNode());
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();
    if (context->flag_quit_)
    {
      printf("bye!\n");
      break;
    }
  }

  SaveDBs();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  SaveDBs();
  return DB_SUCCESS;
}

void ExecuteEngine::SaveDBs()
{
  for(std::pair<std::string, DBStorageEngine*> db_info : dbs_)
  {
    std::string dbs_name_file = db_info.first;
    fstream out_file_stream(dbs_name_file, ios::out);
    out_file_stream << db_info.second << endl;
    out_file_stream.close();
  }
}