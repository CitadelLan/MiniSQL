#include "storage/table_heap.h"

#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 10000;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(),
                                            nullptr, nullptr, nullptr);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    auto *fields =
        new Fields{Field(TypeId::kTypeInt, i),
                   Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   // 析构时free
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    if (row_values.find(row.GetRowId().Get()) != row_values.end()) {
      std::cout << row.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    } else {
      row_values.emplace(row.GetRowId().Get(), fields);
      size++;
    }
    delete[] characters;
  }

  ASSERT_EQ(row_nums, row_values.size());
  ASSERT_EQ(row_nums, size);
  for (auto row_kv : row_values) {
    size--;
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    delete row_kv.second;
  }
  ASSERT_EQ(size, 0);

  delete table_heap;
  delete bpm_;
  delete disk_mgr_;
}

TEST(TableHeapTest, MyTHTest)
{
  // TableHeap 初始化
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 1000;
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(),
                                            nullptr, nullptr, nullptr);
  // 建立rows
  std::unordered_map<int64_t, Fields *> row_values;
  std::vector<RowId> RID;
  float tmp = 1;
  for (int i = 0; i < row_nums; i++) {
    int32_t len = 20;
    char *characters = "kestrel";
    auto *fields =
        new Fields{Field(TypeId::kTypeInt, i),
                   Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   // 析构时free
                   Field(TypeId::kTypeFloat, tmp)};
    Row row(*fields);
    /* 检验InsertTuple UpdateTuple */
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));

    RID.push_back(row.GetRowId());
    row_values.emplace(row.GetRowId().Get(), fields);
  }

  /* 检验Begin() End() it++(侧面检验++it) */
  int i = 0;
  for(auto it = table_heap->Begin(nullptr); it != table_heap->End(); it++)
  {
    if(i >= 60)
    {
      int a = 0;
    }
    EXPECT_EQ(it->GetRowId(), RID[i]);
    i++;
  }
  /* 创建新row */
  int32_t len = 20;
  char *characters = "kestrel";
  auto *fields =
      new Fields{Field(TypeId::kTypeInt, 1),
                 Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                 // 析构时free
                 Field(TypeId::kTypeFloat, tmp)};
  Row row(*fields);
  /* 检验UpdateTuple() */
  auto HRID = table_heap->Begin(nullptr)->GetRowId();
  ASSERT_TRUE(table_heap->UpdateTuple(row, HRID, nullptr));

  table_heap->FreeTableHeap();
  delete table_heap;
  delete bpm_;
  delete disk_mgr_;
}
