#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  //ASSERT(false, "Not Implemented yet");
  return 3 * sizeof(uint32_t) +
         (sizeof(table_id_t) + sizeof(page_id_t)) * table_meta_pages_.size() +
         (sizeof(index_id_t) + sizeof(page_id_t)) * index_meta_pages_.size();
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
    //ASSERT(false, "Not Implemented yet");
    /* 需要初始化 */
    if (init)
    {
      std::atomic_init(&next_table_id_,0);
      std::atomic_init(&next_index_id_,0);
      catalog_meta_ = CatalogMeta::NewInstance();
    }
    /* 不需要初始化 */
    else {
      /* 1.1. 读取CATALOG_META_PAGE里的信息（反序列化） */
      auto meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
      catalog_meta_ = CatalogMeta::DeserializeFrom(meta_page->GetData());

      /* 1.2. 更新CatalogManager里table相关信息 */
      for (auto it : catalog_meta_->table_meta_pages_)
      {
        dberr_t verify = LoadTable(it.first, it.second);
        ASSERT(verify == DB_SUCCESS, "Error in CatalogManager::LoadTable()");
      }

      /* 1.3. 更新CatalogManager里index相关信息 */
      for (auto it : catalog_meta_->index_meta_pages_)
      {
        dberr_t verify = LoadIndex(it.first, it.second);
        ASSERT(verify == DB_SUCCESS, "Error in CatalogManager::LoadIndex()");
      }

      buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID,false);
    }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 * 类似 next-fit 方法，在下一位置新建表
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  /* 0. 如果可以找到该表，则不应该重复创建 */
  if (table_names_.find(table_name) != table_names_.end())
    return DB_TABLE_ALREADY_EXIST;

  table_id_t table_id = next_table_id_;
  next_table_id_++;
  page_id_t page_id;
  auto table_meta_page = buffer_pool_manager_->NewPage(page_id);
  auto table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);
  auto table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), schema);

  table_meta->SerializeTo(table_meta_page->GetData());

  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);

  /* 缺少对unique和主键的特判 */

  /* 更新CatalogMeta、CatalogManager 的 map映射 */
  catalog_meta_->table_meta_pages_[table_id] = page_id;
  table_names_[table_name] = table_id;
  tables_[table_id] = table_info;

  /* 因为对CatalogMeta造成了改动，所以需要将CatalogMeta写回 */
  auto catalogPg = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData();
  catalog_meta_->SerializeTo(catalogPg);

  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  buffer_pool_manager_->UnpinPage(page_id, true);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if (table_names_.find(table_name) == table_names_.end())
    return DB_TABLE_NOT_EXIST;

  table_id_t table_id = table_names_[table_name];
  table_info = tables_[table_id];

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if (tables_.empty())
    return DB_FAILED;

  for (auto &it : tables_)
    tables.push_back(it.second);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  /* 0. 检查表和索引是否已存在 */
  if (table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  if ((index_names_[table_name].find(index_name) != index_names_[table_name].end())) return DB_INDEX_ALREADY_EXIST;

  auto schema = tables_[table_names_[table_name]]->GetSchema();
  auto table_info = tables_[table_names_[table_name]];
  std::vector<uint32_t> key_map;

  /* 1. 得到并存储columnName与其在schema中下标的对应关系 */
  for (auto &it : index_keys)
  {
    uint32_t col_idx;
    if (schema->GetColumnIndex(it, col_idx) == DB_COLUMN_NAME_NOT_EXIST) return DB_COLUMN_NAME_NOT_EXIST;
    key_map.push_back(col_idx);
  }

  /* 3. 初始化index_info并建立映射关系 */
  auto index_id = catalog_meta_->GetNextIndexId();
  index_names_[table_name][index_name] = index_id;
  page_id_t page_id;
  auto index_meta_page =  buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->index_meta_pages_[index_id] = page_id;
  buffer_pool_manager_->UnpinPage(page_id, true);
  auto index_meta = IndexMetadata::Create(index_id, index_name, table_names_[table_name], key_map);
  index_meta->SerializeTo(index_meta_page->GetData());
  index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  indexes_[index_id] = index_info;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if (table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  if (index_names_.find(table_name) != index_names_.end())
  {
    if(index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end())
        return DB_INDEX_NOT_FOUND;
  }
  else
    return DB_INDEX_NOT_FOUND;

  auto index_id = index_names_.at(table_name).at(index_name);
  index_info = indexes_.at(index_id);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement（Problematic）
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  if (table_names_.find(table_name) == table_names_.end())
    return DB_TABLE_NOT_EXIST;

  for (auto &it : indexes_)
    if (it.second->GetTableInfo()->GetTableName() == table_name)
      indexes.emplace_back(it.second);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if (table_names_.find(table_name) == table_names_.end())
    return DB_TABLE_NOT_EXIST;

  auto table_id = table_names_[table_name];
  table_names_.erase(table_name);
  page_id_t page_id = catalog_meta_->table_meta_pages_[table_id];
  catalog_meta_->table_meta_pages_.erase(table_id);
  buffer_pool_manager_->DeletePage(page_id);
  tables_.erase(table_id);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if (table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  if (index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end()) return DB_INDEX_NOT_FOUND;

  auto index_id = index_names_[table_name][index_name];
  index_names_.erase(table_name);
  page_id_t page_id = catalog_meta_->index_meta_pages_[index_id];
  catalog_meta_->index_meta_pages_.erase(index_id);
  buffer_pool_manager_->DeletePage(page_id);
  indexes_.erase(index_id);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  auto meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(meta_page->GetData());

  if (!buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)
      && !buffer_pool_manager_->FlushPage(INDEX_ROOTS_PAGE_ID))
    return DB_FAILED;

  // LOG(INFO) << "CATALOG FLUSHED" << endl;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 * 1. 建立管理Table整体信息的对象
 * 2. 更新 CatalogManager 里的信息 (map for tables)
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  /* 0. 未能够找到对应的table_id，说明该表不存在 */
  const std::map<table_id_t, page_id_t> &tmp = catalog_meta_->table_meta_pages_;
  if(tmp.find(table_id) == tmp.end())
    return DB_TABLE_NOT_EXIST;

  /* 1. 建立管理Table整体信息的对象 */
  auto table_info = TableInfo::Create();
  /* 1.1. 取得含有表元信息的对应页，做反序列化 */
  auto table_meta_page = buffer_pool_manager_->FetchPage(page_id);
  TableMetadata *table_meta = nullptr;
  TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_meta);
  /* 1.2. 建立该表的TableHeap */
  auto table_heap = TableHeap::Create(buffer_pool_manager_,
                                        table_meta->GetFirstPageId(), table_meta->GetSchema(),
                                        log_manager_, lock_manager_);
  /* 1.3. 初始化TableInfo */
  table_info->Init(table_meta, table_heap);

  /* 2.更新 CatalogManager 里的信息 (map for tables) */
  table_names_[table_meta->GetTableName()] = table_id;
  tables_[table_id] = table_info;

  buffer_pool_manager_->UnpinPage(page_id,false);

  return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 * 1. 建立管理Index整体信息的对象
 * 2. 更新 CatalogManager 里的信息 (map for indexes)
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  /* 0. 能够找到对应的index_id，说明该索引已存在 */
  const std::map<index_id_t, page_id_t> &imp = catalog_meta_->index_meta_pages_;

  /* 1. 建立管理Index整体信息的对象 */
  auto index_info = IndexInfo::Create();
  /* 1.1. 取得含有索引元信息的对应页，做反序列化 */
  auto index_meta_page = buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata *index_meta;
  IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta);
  /* 1.2. 初始化IndexInfo */
  table_id_t table_id = index_meta->GetTableId();
  index_info->Init(index_meta, tables_[table_id], buffer_pool_manager_);

  /* 2.更新 CatalogManager 里的信息 (map for indexes) */
  indexes_[index_id] = index_info;
  index_names_[tables_[table_id]->GetTableName()][index_info->GetIndexName()] = index_id;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if (catalog_meta_->table_meta_pages_.find(table_id) == catalog_meta_->table_meta_pages_.end())
    return DB_TABLE_NOT_EXIST;

  table_info = tables_[table_id];

  return DB_SUCCESS;
}